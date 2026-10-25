import pytest

from backend.placement import fits_on_gpus, required_vram_bytes


def test_weights_and_kv_cache_scale_with_precision_and_length():
    base = dict(params_billion=7, layers=32, kv_heads=8, head_dim=128, max_seq_len=4096, batch=1)
    fp16 = required_vram_bytes(dtype="fp16", **base)
    int4 = required_vram_bytes(dtype="int4", **base)
    longer = required_vram_bytes(dtype="fp16", **{**base, "max_seq_len": 8192})
    assert int4 < fp16 < longer


def test_unknown_or_missing_sizes_are_rejected():
    with pytest.raises(ValueError):
        required_vram_bytes(params_billion=0, dtype="fp16", layers=1, kv_heads=1, head_dim=1, max_seq_len=1, batch=1)
    with pytest.raises(ValueError):
        required_vram_bytes(params_billion=7, dtype="fp7", layers=1, kv_heads=1, head_dim=1, max_seq_len=1, batch=1)


def test_unmeasured_gpu_memory_is_never_available():
    need = 40 * 1024**3
    decision = fits_on_gpus(required_bytes=need, tensor_parallel=1, free_mib_per_gpu=[None])
    assert decision["fits"] is False


def test_sharded_fit_uses_per_gpu_free_memory():
    need = 160 * 1024**3
    assert fits_on_gpus(required_bytes=need, tensor_parallel=4, free_mib_per_gpu=[10000] * 4)["fits"] is False
    assert fits_on_gpus(required_bytes=need, tensor_parallel=4, free_mib_per_gpu=[50000] * 4)["fits"] is True


def test_too_few_gpus_for_parallelism_is_rejected():
    decision = fits_on_gpus(required_bytes=1, tensor_parallel=4, free_mib_per_gpu=[90000, 90000])
    assert decision["fits"] is False
