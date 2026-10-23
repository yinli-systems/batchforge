"""CPU-only adapter for the exact vLLM 0.10.2 GPT-OSS chat formatter.

The /tokenize chat endpoint uses a different HF-template path in this version.
Use the same installed engine virtualenv as the server, with GPUs hidden. This
adapter calls its actual Harmony helpers; it never constructs an approximate
HF template or performs inference. Engine usage equality remains mandatory.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import importlib
from importlib import metadata, util
import os
from pathlib import Path


class GptOssHarmonyCounter:
    def __init__(self, *, render_date=None):
        if os.environ.get("CUDA_VISIBLE_DEVICES") != "":
            raise ValueError("Hide GPUs with CUDA_VISIBLE_DEVICES='' for the CPU-only Harmony counter")
        if metadata.version("vllm") != "0.10.2":
            raise ValueError("This counter is verified only for the actual vLLM 0.10.2 serving path")
        self.render_date = render_date or datetime.now().date().isoformat()
        if date.fromisoformat(self.render_date).isoformat() != self.render_date:
            raise ValueError("Harmony render date must be YYYY-MM-DD")
        self.harmony = importlib.import_module("vllm.entrypoints.harmony_utils")
        serving = util.find_spec("vllm.entrypoints.openai.serving_chat")
        if serving is None or not serving.origin or not self.harmony.__file__:
            raise ValueError("Installed serving formatter source must be inspectable")
        self.counter_metadata = {
            "type": "vllm-0.10.2-gptoss-harmony",
            "vllm_version": metadata.version("vllm"),
            "openai_harmony_version": metadata.version("openai-harmony"),
            "harmony_utils_sha256": hashlib.sha256(Path(self.harmony.__file__).read_bytes()).hexdigest(),
            "serving_chat_sha256": hashlib.sha256(Path(serving.origin).read_bytes()).hexdigest(),
            "adapter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "render_date": self.render_date,
            "render_date_scope": "Counter host local calendar date; server uses its own local current date. Usage equality must still pass.",
            "reasoning_effort": None,
            "tools": None,
            "request_scope": "Text messages without tools or per-request formatter overrides",
            "formatter": "get_system_message + get_developer_message + parse_chat_input + render_for_completion",
            "not_equivalent_endpoint": "/tokenize with TokenizeChatRequest",
        }
        # Existing fitter code only hashes this attribute for HF tokenizers.
        # The manifest records separate formatter provenance for this adapter.
        self.chat_template = None

    def apply_chat_template(self, messages, *, tokenize=True, add_generation_prompt=True):
        if tokenize is not True or add_generation_prompt is not True:
            raise ValueError("Harmony counter only supports tokenized assistant generation prompts")
        if not isinstance(messages, list) or not messages:
            raise ValueError("At least one text message is required")
        for item in messages:
            if (not isinstance(item, dict) or set(item) != {"role", "content"}
                    or item["role"] not in {"system", "developer", "user", "assistant"}
                    or not isinstance(item["content"], str)):
                raise ValueError("Counter accepts plain text role/content messages only")
        # Exact order and options from OpenAIServingChat._make_request_with_harmony.
        rendered = [self.harmony.get_system_message(reasoning_effort=None,
            browser_description=None, python_description=None, start_date=self.render_date),
            self.harmony.get_developer_message(tools=None)]
        for item in messages:
            rendered.extend(self.harmony.parse_chat_input(item))
        return self.harmony.render_for_completion(rendered)
