import pytest

from scripts.trace_intervals import category, summarize_events


def event(start, duration, name, device=0):
    return {'ph': 'X', 'cat': 'kernel', 'name': name, 'ts': start, 'dur': duration, 'args': {'device': device}}


def test_overlap_is_partitioned_and_never_added_as_wall_time():
    result = summarize_events([event(0, 10, 'gemm'), event(2, 3, 'gemm'), event(5, 10, 'ncclAllReduce'),
                               event(20, 5, 'flash_attention')])
    assert result['any_kernel_interval_union_seconds'] == pytest.approx(20e-6)
    assert result['first_kernel_to_last_kernel_span_seconds'] == pytest.approx(25e-6)
    assert result['categories']['gemm']['interval_union_seconds'] == pytest.approx(10e-6)
    assert result['categories']['gemm']['summed_kernel_durations_seconds_not_wall_time'] == pytest.approx(13e-6)
    assert result['cross_category_overlap_union_seconds'] == pytest.approx(5e-6)
    assert result['same_category_concurrency_union_seconds'] == pytest.approx(3e-6)
    partition = sum(v['exclusive_interval_seconds'] for v in result['categories'].values()) + result['cross_category_overlap_union_seconds']
    assert partition == pytest.approx(result['any_kernel_interval_union_seconds'])


def test_cpu_events_excluded_and_adjacent_intervals_not_overlapping():
    result = summarize_events([event(0, 10, 'gemm'), event(10, 10, 'nccl'),
                               {'ph': 'X', 'cat': 'cpu_op', 'ts': 0, 'dur': 999, 'name': 'aten::mm'}])
    assert result['kernel_count'] == 2
    assert result['cross_category_overlap_union_seconds'] == 0
    assert category('cutlass::FlashAttnFwdSm90') == 'attention'
    assert category('void flash::flash_fwd_kernel<Flash_fwd_kernel_traits>') == 'attention'
    assert category('fused_moe_gemm') == 'moe_and_expert'


def test_missing_gpu_events_bad_intervals_and_multiple_devices_rejected():
    for values in ([], [event(0, -1, 'gemm')], [event(float('inf'), 1, 'gemm')],
                   [event(0, 1, 'gemm', 0), event(0, 1, 'gemm', 1)]):
        with pytest.raises(ValueError):
            summarize_events(values)


def test_individual_kernel_union_preserves_overlap_and_bounded_name_evidence():
    name = 'fused_moe_kernel' + 'x' * 600
    result = summarize_events([event(0, 10, name), event(5, 10, name), event(20, 2, 'nccl')])
    top = result['top_kernels_by_individual_interval_union'][0]
    assert top['kernel_count'] == 2
    assert top['category'] == 'moe_and_expert'
    assert len(top['name_sample']) == 512
    assert len(top['full_name_sha256']) == 64
    assert top['interval_union_seconds'] == pytest.approx(15e-6)
    assert top['summed_durations_seconds_not_wall_time'] == pytest.approx(20e-6)
