import json
from ops.ha.retune_full_gpu import worker_candidate, logged_capacity, candidate_manifest_path
from ops.supervise import NAME


def test_retune_preserves_runtime_identity_credentials_and_endpoint():
    profile = {'profile_id': 'old', 'runtime_fingerprint': 'immutable-runtime',
               'prefill_tokens_per_second': 1000, 'decode_tokens_per_second': 50,
               'hourly_cost_usd': 2, 'calibration_label': 'measured-old', 'cost_source': 'old'}
    source = {'SCHEDULER_PROFILE_JSON': json.dumps(profile), 'BATCHFORGE_WORKER_TOKEN': 'test-placeholder',
              'CONTROL_PLANE_URLS': 'http://127.0.0.1:8082,http://127.0.0.1:8083', 'MODEL_NAME': 'openai/gpt-oss-120b'}
    result = worker_candidate(source, 64, 16.1)
    assert result['BATCHFORGE_WORKER_TOKEN'] == source['BATCHFORGE_WORKER_TOKEN']
    assert result['CONTROL_PLANE_URLS'] == source['CONTROL_PLANE_URLS']
    assert result['MODEL_NAME'] == source['MODEL_NAME']
    assert result['WORKER_TOKEN_BUDGET'] == str(64*18440)
    parsed = json.loads(result['SCHEDULER_PROFILE_JSON'])
    assert parsed['runtime_fingerprint'] == profile['runtime_fingerprint']
    assert parsed['hourly_cost_usd'] == 16.1/64
    assert parsed['prefill_tokens_per_second'] == 1000
    assert 'not-recalibrated' in parsed['calibration_label']
    assert json.loads(source['SCHEDULER_PROFILE_JSON']) == profile


def test_startup_capacity_uses_most_conservative_rank_and_context():
    assert logged_capacity('Maximum concurrency for 18,440 tokens per request: 71.2x\nMaximum concurrency for 18,440 tokens per request: 63.9x') == 63.9
    assert logged_capacity('Maximum concurrency for 4,096 tokens per request: 512.0x') is None


def test_candidate_manifest_obeys_supervisor_location_and_name(tmp_path):
    path = candidate_manifest_path(tmp_path, 'full120-seq64-batch4096-v1')
    assert path.parent == tmp_path / '.runtime'
    assert NAME.fullmatch(path.stem)
    assert path.suffix == '.json'
