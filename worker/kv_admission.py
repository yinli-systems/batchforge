"""Opt-in, single-owner Qwen/vLLM token admission; not a GPU KV allocator.

Only the operator chooses the budget. A private /tokenize call uses the same
ordinary-chat preprocessing path as vLLM0.10.2 chat completion for this model.
GPT-OSS's separate Harmony path is deliberately unsupported.
"""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
import math


MODEL = 'Qwen/Qwen3-235B-A22B-Instruct-2507-FP8'
PROTOCOL = 'vllm-0.10.2-qwen-chat'


class AdmissionPaused(Exception):
    pass


class TokenizerProtocolError(ValueError):
    pass


@dataclass(eq=False)
class _Ticket:
    amount: int
    future: asyncio.Future
    granted: bool = False


class WeightedLimiter:
    """FIFO, event-loop-local reservations with synchronous cancellation cleanup.

There is no await between changing accounting and notifying a waiter. Even a
grant cancelled before its task resumes is released in the context's finally.
Pause is latched; operator intervention is required rather than guessing that
an HTTP disconnect synchronously freed engine KV blocks.
"""
    def __init__(self, budget, observer=None):
        if type(budget) is not int or budget < 1:
            raise ValueError('Token budget must be positive')
        self.budget, self.used, self.peak = budget, 0, 0
        self.paused = False
        self._queue, self._observer = deque(), observer

    @property
    def waiting(self):
        return len(self._queue)

    def _notify(self):
        if self._observer:
            self._observer(self.used, self.waiting, self.peak, self.paused)

    def _drain(self):
        while self._queue and not self.paused:
            ticket = self._queue[0]
            if ticket.future.cancelled():
                self._queue.popleft()
                continue
            if self.used + ticket.amount > self.budget:
                break
            self._queue.popleft()
            self.used += ticket.amount
            self.peak = max(self.peak, self.used)
            ticket.granted = True
            ticket.future.set_result(None)
        self._notify()

    def pause(self):
        self.paused = True
        while self._queue:
            ticket = self._queue.popleft()
            if not ticket.future.done():
                ticket.future.set_exception(AdmissionPaused())
        self._notify()

    @asynccontextmanager
    async def reserve(self, amount):
        if type(amount) is not int or not 1 <= amount <= self.budget:
            raise ValueError('Reservation does not fit token budget')
        if self.paused:
            raise AdmissionPaused()
        ticket = _Ticket(amount, asyncio.get_running_loop().create_future())
        self._queue.append(ticket)
        try:
            self._drain()
            await ticket.future
            if self.paused:
                raise AdmissionPaused()
            yield
        finally:
            if ticket in self._queue:
                self._queue.remove(ticket)
            if ticket.granted:
                self.used -= ticket.amount
                ticket.granted = False
            # Consume a paused waiter's exception even when cancellation wins
            # the race before the task can resume and retrieve that exception.
            if ticket.future.done() and not ticket.future.cancelled():
                ticket.future.exception()
            self._drain()


def tokenizer_body(body, model):
    if model != MODEL or body.get('model') != model:
        raise TokenizerProtocolError('Unsupported exact tokenizer model')
    messages = body.get('messages')
    if not isinstance(messages, list) or not 1 <= len(messages) <= 128:
        raise TokenizerProtocolError('Tokenizer requires text chat messages')
    for message in messages:
        if (not isinstance(message, dict) or message.get('role') not in {'system','user','assistant'}
                or not isinstance(message.get('content'), str)
                or set(message) - {'role','content','name'}):
            raise TokenizerProtocolError('Unsupported tokenizer message shape')
    return {'model': model, 'messages': messages, 'add_generation_prompt': True,
            'continue_final_message': False, 'add_special_tokens': False,
            'return_token_strs': False}


def token_count(value, max_model_len):
    if not isinstance(value, dict):
        raise TokenizerProtocolError('Tokenizer result is not an object')
    count, tokens = value.get('count'), value.get('tokens')
    if (type(count) is not int or not 0 < count <= max_model_len
            or type(value.get('max_model_len')) is not int or value['max_model_len'] != max_model_len
            or not isinstance(tokens, list) or len(tokens) != count
            or any(type(token) is not int or not 0 <= token < 2**31 for token in tokens)):
        raise TokenizerProtocolError('Tokenizer count, IDs or context bound differ')
    return count


def reservation(prompt_tokens, maximum_output, context, block_size=16):
    if (type(prompt_tokens) is not int or prompt_tokens < 1
            or type(maximum_output) is not int or maximum_output < 1
            or prompt_tokens + maximum_output > context or block_size != 16):
        raise ValueError('Exact prompt plus requested output exceeds context')
    return ((prompt_tokens + maximum_output + block_size - 1) // block_size) * block_size


def engine_quiescent(metrics):
    """Only used before this worker's first adaptive admission, never to resume.

Both V0/V1-style metric labels and unlabelled gauges are accepted; unknown,
negative, NaN, absent, or nonzero populations fail closed.
"""
    values = {'vllm:num_requests_running': [], 'vllm:num_requests_waiting': []}
    for line in metrics.splitlines():
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        if len(parts) != 2:
            continue
        name = parts[0].split('{', 1)[0]
        if name in values:
            try:value = float(parts[1])
            except ValueError:return False
            if not math.isfinite(value) or value < 0:return False
            values[name].append(value)
    return all(population and sum(population) == 0 for population in values.values())
