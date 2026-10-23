import pytest

from ops.pod2.capture_observation import contained, instant


def test_only_fully_contained_samples_survive_including_exact_boundary():
    samples = [
        {'started_at': '2026-10-23T21:39:17+00:00', 'finished_at': '2026-10-23T21:39:19+00:00'},
        {'started_at': '2026-10-23T21:39:18+00:00', 'finished_at': '2026-10-23T21:40:45+00:00'},
        {'started_at': '2026-10-23T21:40:44+00:00', 'finished_at': '2026-10-23T21:40:46+00:00'},
    ]
    assert contained(samples, instant('2026-10-24T01:39:18+04:00'),
                     instant('2026-10-23T21:40:45+00:00')) == samples[1:2]


def test_naive_or_reversed_measurement_windows_are_rejected():
    with pytest.raises(ValueError, match='timezone'):
        instant('2026-10-23T21:40:45')
    with pytest.raises(ValueError, match='follow'):
        contained([], instant('2026-10-23T21:40:45+00:00'), instant('2026-10-23T21:39:18+00:00'))
