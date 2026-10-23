"""Exact llama.cpp formatter counter and uncached natural-inference probe.

The live server's /apply-template uses the inference chat parser. Token counts
come from its /tokenize endpoint, not a potentially different HF formatter.
Counting performs no inference; probe() is the only generation operation.
The CLI preserves an exclusive .started marker after success or failure; an
uncertain generation is never replayed automatically under the same report name.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import httpx


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def start_probe_marker(report: Path, *, input_sha256: str):
    """Fence uncertain CLI outcomes before the one inference POST. Never retry."""
    import os
    marker = report.with_name(report.name + '.started')
    marker.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w') as out:
        json.dump({'started_at': datetime.now(timezone.utc).isoformat(),
            'input_sha256': input_sha256, 'automatic_retry': False}, out)
        out.write('\n')
    return marker


class LlamaPromptCounter:
    def __init__(self, url: str, *, model: str, commit: str, revision: str,
                 model_sha256: str | None = None, binary_sha256: str | None = None,
                 transport=None):
        parsed = urlsplit(url)
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or parsed.path not in {'', '/'}):
            raise ValueError('Use an explicit engine origin without credentials or path')
        if not re.fullmatch(r'[0-9a-f]{40}', commit) or not re.fullmatch(r'[0-9a-f]{40}', revision):
            raise ValueError('Pin full runtime and model revisions')
        for checksum in (model_sha256, binary_sha256):
            if checksum is not None and not re.fullmatch(r'[0-9a-f]{64}', checksum):
                raise ValueError('Invalid supplied asset SHA256')
        self.model = model
        self.client = httpx.Client(base_url=url.rstrip('/'), trust_env=False,
            timeout=30, transport=transport)
        try:
            models = self._get('/v1/models')['data']
            if not any(row.get('id') == model for row in models):
                raise ValueError('The exact model is absent from the engine catalog')
            props = self._get('/props')
            self.chat_template = props.get('chat_template')
            if not isinstance(self.chat_template, str) or not self.chat_template:
                raise ValueError('The actual server chat template is unavailable')
            context = props.get('default_generation_settings', {}).get('n_ctx')
            if type(context) is not int or context < 1:
                raise ValueError('The actual server context capacity is unavailable')
            self.context_capacity = context
            self.counter_metadata = {
                'type': 'llama-cpp-apply-template-tokenize', 'llama_cpp_commit': commit,
                'model_revision': revision, 'model': model,
                'supplied_model_sha256': model_sha256, 'supplied_binary_sha256': binary_sha256,
                'actual_server_chat_template_sha256': digest(self.chat_template.encode()),
                'actual_server_context_capacity': context,
                'actual_server_build_info': props.get('build_info'),
                'counter_source_sha256': digest(Path(__file__).read_bytes()),
                'formatter': '/apply-template: same oaicompat_chat_params_parse path as inference',
                'tokenizer': '/tokenize add_special=false, parse_special=true',
                'cache_policy_for_probes': 'cache_prompt=false; actual cached_tokens must equal zero',
                'scope': 'Plain text messages without tools or per-request formatter overrides',
                'supplied_asset_hash_scope': 'Operator provenance; server identity/asset verification is separate',
            }
        except Exception:
            self.client.close()
            raise

    def _get(self, path):
        response = self.client.get(path)
        response.raise_for_status()
        return response.json()

    def _post(self, path, body, *, timeout=None):
        response = self.client.post(path, json=body, timeout=timeout or 30)
        response.raise_for_status()
        return response.json()

    def apply_chat_template(self, messages, *, tokenize=True, add_generation_prompt=True):
        if tokenize is not True or add_generation_prompt is not True:
            raise ValueError('Only tokenized assistant-generation prompts are supported')
        if not isinstance(messages, list) or not messages:
            raise ValueError('At least one plain text message is required')
        for row in messages:
            if (not isinstance(row, dict) or set(row) != {'role', 'content'}
                    or row['role'] not in {'system', 'developer', 'user', 'assistant'}
                    or not isinstance(row['content'], str)):
                raise ValueError('Only plain text role/content messages are supported')
        prompt = self._post('/apply-template', {'model': self.model, 'messages': messages,
            'add_generation_prompt': True}).get('prompt')
        if not isinstance(prompt, str):
            raise ValueError('Server formatter did not return a text prompt')
        tokens = self._post('/tokenize', {'content': prompt, 'add_special': False,
            'parse_special': True}).get('tokens')
        if not isinstance(tokens, list) or any(type(token) is not int or token < 0 for token in tokens):
            raise ValueError('Server tokenizer did not return integer token IDs')
        return tokens

    def calibrate(self, messages, observed_prompt_tokens: int):
        if type(observed_prompt_tokens) is not int or observed_prompt_tokens < 1:
            raise ValueError('A positive observed inference prompt count is required')
        count = len(self.apply_chat_template(messages))
        if count != observed_prompt_tokens:
            raise ValueError('Server counter differs from observed inference usage')
        return {'observed_inference_prompt_tokens': observed_prompt_tokens,
            'counter_prompt_tokens': count, 'matched': True}

    def probe(self, body: dict, *, admission_context: int, timeout: float = 1800):
        allowed = {'model', 'messages', 'max_tokens', 'temperature', 'top_p', 'seed', 'stream', 'cache_prompt'}
        if not isinstance(body, dict) or set(body) - allowed:
            raise ValueError('Unsupported probe keys may change the server formatter')
        if body.get('model') != self.model:
            raise ValueError('Probe model must match the verified engine catalog')
        if any(key in body for key in ('ignore_eos', 'min_tokens', 'stop_token_ids', 'logit_bias')):
            raise ValueError('The uncached probe measures natural generation only')
        cap = body.get('max_tokens')
        if type(cap) is not int or cap < 1:
            raise ValueError('A positive output cap is required')
        prompt_count = len(self.apply_chat_template(body['messages']))
        if (type(admission_context) is not int or admission_context > self.context_capacity
                or prompt_count + cap > admission_context):
            raise ValueError('Request does not fit the explicit worker context reservation')
        if body.get('stream', False) is not False:
            raise ValueError('The natural probe uses a complete non-streaming response')
        request = {**body, 'cache_prompt': False}
        started_at = datetime.now(timezone.utc).isoformat()
        started = time.monotonic()
        value = self._post('/v1/chat/completions', request, timeout=timeout)
        elapsed = time.monotonic() - started
        if not isinstance(value, dict) or not isinstance(value.get('usage'), dict):
            raise ValueError('Missing engine token usage')
        usage = value['usage']
        actual = usage.get('prompt_tokens')
        outputs = usage.get('completion_tokens')
        details = usage.get('prompt_tokens_details')
        cached = details.get('cached_tokens') if isinstance(details, dict) else None
        if (value.get('model') != self.model or type(actual) is not int or actual != prompt_count
                or type(outputs) is not int or not 0 < outputs <= cap
                or type(cached) is not int or cached != 0):
            raise ValueError('Actual inference usage or uncached contract did not match')
        choices = value.get('choices')
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise ValueError('Missing engine choice/message')
        choice = choices[0]
        message = choice.get('message')
        if not isinstance(message, dict) or choice.get('finish_reason') not in {'stop', 'length'}:
            raise ValueError('Incomplete or failed engine completion')
        for key in ('content', 'reasoning_content', 'reasoning'):
            if message.get(key) is not None and not isinstance(message[key], str):
                raise ValueError('Invalid response text types')
        content = message.get('content') if message.get('content') is not None else ''
        reasoning = message.get('reasoning_content')
        if reasoning is None:
            reasoning = message.get('reasoning')
        if reasoning is None:
            reasoning = ''
        report = {'scope': 'engine-only natural-generation probe, not a durable platform completion',
            'passed': True, 'model': self.model, 'measurement_started_at': started_at,
            'finished_at': datetime.now(timezone.utc).isoformat(), 'elapsed_seconds': elapsed,
            'rendered_prompt_tokens': prompt_count, 'output_cap': cap, 'usage': usage,
            'worker_context_reservation': admission_context,
            'actual_server_context_capacity': self.context_capacity,
            'per_request_cache_prompt': False, 'cached_prompt_tokens': cached,
            'source_body_sha256': digest(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()),
            'natural_generation_mode': True, 'natural_stop': choice.get('finish_reason') == 'stop',
            'finish_reason': choice.get('finish_reason'), 'final_text_nonempty': bool(content.strip()),
            'final_text_sha256': digest(content.encode()), 'reasoning_text_sha256': digest(reasoning.encode()),
            'server_timings': value.get('timings'), 'prompt_counter': self.counter_metadata,
            'limitations': ['No task quality or fixed output length is implied by inference success',
                'Server durations are not isolated CUDA kernel timing',
                'Raw response text is omitted from this report']}
        return report, value

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def parse_timing_pairs(text: str) -> list[dict]:
    """Parse only owned appended server logs; no inference or process discovery.

    These are engine CPU/GPU durations, not CUDA kernel times. With a single
    exclusive FIFO client, processed prompt tokens can be matched to each
    request to detect KV prefix reuse. Reject ambiguous or partial windows.
    """
    pattern = re.compile(r"slot print_timing:\s*id\s+\d+\s*\|\s*task\s+(\d+)\s*\|\s*"
        r"(prompt eval time|eval time)\s*=\s*([\d.]+) ms /\s*(\d+) tokens")
    tasks = {}
    for task, kind, duration, count in pattern.findall(text):
        row = tasks.setdefault(int(task), {'task_id': int(task)})
        prefix = 'prefill' if kind == 'prompt eval time' else 'decode'
        if prefix+'_ms' in row:
            raise ValueError('Duplicate task timing makes the capture ambiguous')
        row[prefix+'_ms'] = float(duration)
        row[prefix+'_tokens'] = int(count)
    if not tasks or any('prefill_ms' not in row or 'decode_ms' not in row for row in tasks.values()):
        raise ValueError('Incomplete prompt/decode timing window')
    return list(tasks.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--model', default='openai/gpt-oss-120b')
    parser.add_argument('--commit', required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--model-sha256')
    parser.add_argument('--binary-sha256')
    parser.add_argument('--calibration', type=Path, required=True,
        help='JSON with messages and actual observed_prompt_tokens, derived from a real response')
    parser.add_argument('--context-limit', type=int, default=18440)
    parser.add_argument('--probe-input', type=Path, help='JSON body or {body: ...}; sends one natural request')
    parser.add_argument('--report', type=Path)
    parser.add_argument('--private-response', type=Path)
    parser.add_argument('--dataset', type=Path)
    parser.add_argument('--inputs', type=int, nargs='+', default=[128, 512, 2048, 8192, 16384])
    parser.add_argument('--outputs', type=int, nargs='+', default=[32, 128, 512, 2048])
    parser.add_argument('--repeats', type=int, default=1)
    parser.add_argument('--seed', type=int, default=20261024)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--manifest', type=Path)
    args = parser.parse_args()
    if args.probe_input and not args.report:
        parser.error('--probe-input requires --report')
    if not args.probe_input and not all([args.dataset, args.output, args.manifest]):
        parser.error('Matrix generation requires --dataset, --output and --manifest')
    if args.probe_input and (args.report.exists()
            or args.report.with_name(args.report.name+'.started').exists()
            or (args.private_response and args.private_response.exists())):
        parser.error('Existing probe output prevents uncertain duplicate inference; select a new explicit experiment')
    calibration_bytes = args.calibration.read_bytes()
    calibration = json.loads(calibration_bytes)
    with LlamaPromptCounter(args.url, model=args.model, commit=args.commit, revision=args.revision,
            model_sha256=args.model_sha256, binary_sha256=args.binary_sha256) as counter:
        if args.context_limit > counter.context_capacity:
            parser.error('Requested context exceeds actual server capacity')
        counter.counter_metadata['calibration'] = counter.calibrate(calibration['messages'],
            calibration['observed_prompt_tokens'])
        counter.counter_metadata['calibration_file_sha256'] = digest(calibration_bytes)
        if args.probe_input:
            value = json.loads(args.probe_input.read_text())
            body = value.get('body', value)
            marker = start_probe_marker(args.report, input_sha256=digest(args.probe_input.read_bytes()))
            report, raw = counter.probe(body, admission_context=args.context_limit)
            report['exclusive_start_marker'] = marker.name
            report['automatic_retry'] = False
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + '\n')
            if args.private_response:
                from os import O_CREAT, O_EXCL, O_WRONLY, fdopen, open as private_open
                args.private_response.parent.mkdir(parents=True, exist_ok=True)
                descriptor = private_open(args.private_response, O_CREAT | O_EXCL | O_WRONLY, 0o600)
                with fdopen(descriptor, 'w') as out:
                    json.dump(raw, out, ensure_ascii=False)
                    out.write('\n')
            print(json.dumps({key: value for key, value in report.items() if key != 'prompt_counter'}, indent=2))
        else:
            from scripts.token_workload import build
            payload, manifest = build(args.dataset, counter, model=args.model, revision=args.revision,
                inputs=args.inputs, outputs=args.outputs, repeats=args.repeats, seed=args.seed,
                context_limit=args.context_limit)
            # Direct engine extension, intentionally unsupported by tenant task APIs.
            rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
            for row in rows:
                row['body']['cache_prompt'] = False
            payload = ''.join(json.dumps(row, ensure_ascii=False, separators=(',', ':'))+'\n' for row in rows).encode()
            manifest['workload_sha256'] = digest(payload)
            manifest['cache_policy'] = 'cache_prompt=false for every direct engine request; verify actual cached usage separately'
            for path in (args.output, args.manifest):
                path.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(payload)
            args.manifest.write_text(json.dumps(manifest, indent=2)+'\n')
            print(json.dumps({key: value for key, value in manifest.items() if key != 'examples'}, indent=2))


if __name__ == '__main__':
    main()
