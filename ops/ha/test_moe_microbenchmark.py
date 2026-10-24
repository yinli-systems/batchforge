import math
from argparse import Namespace
import pytest
from ops.ha.moe_microbenchmark import candidates, finite_samples, verify_model


def test_candidate_search_preserves_quantization_and_default():
    base = {'BLOCK_SIZE_M':64,'BLOCK_SIZE_N':128,'BLOCK_SIZE_K':128,'GROUP_SIZE_M':32,'num_warps':4,'num_stages':3}
    values = candidates(base)
    assert len(values) == 9 and values[0] == base
    assert all(v['BLOCK_SIZE_N']==128 and v['BLOCK_SIZE_K']==128 and v['num_warps']==4 for v in values)
    assert base['BLOCK_SIZE_M'] == 64


def test_exact_model_dimensions_are_required():
    config = dict(hidden_size=4096,moe_intermediate_size=1536,num_experts=128,num_experts_per_tok=8,
        quantization_config={'quant_method':'fp8','weight_block_size':[128,128]})
    assert verify_model(config)['num_experts'] == 128
    with pytest.raises(ValueError): verify_model(dict(config, hidden_size=8192))
    with pytest.raises(ValueError): verify_model(dict(config, quantization_config={'quant_method':'fp8'}))


@pytest.mark.parametrize('values',[[],[0],[math.inf],[math.nan],[-1]])
def test_invalid_gpu_timings_cannot_select_a_config(values):
    with pytest.raises(ValueError): finite_samples(values)


def test_timing_selection_uses_median():
    assert finite_samples([2,100,3]) == 3


@pytest.mark.parametrize('live',['engine-qwen235b.json','supervisor-primary.json'])
def test_owned_live_service_is_refused_before_loading_gpu_libraries(tmp_path,monkeypatch,live):
    import ops.ha.moe_microbenchmark as module
    monkeypatch.setattr(module,'owned_record',lambda path:{'pid':42} if path.name==live else None)
    args=Namespace(root=tmp_path,report=tmp_path/'report.json')
    with pytest.raises(RuntimeError,match='stopped|watchdog'):
        module.run(args)
    assert not args.report.exists()
