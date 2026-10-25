from ops.gpu_model_trial import restore_services


class Manager:
    def __init__(self, engine_ready=True):
        self.engine_ready, self.actions = engine_ready, []

    def start_gate(self, service): return True, None

    def invoke(self, service, action): self.actions.append((service, action))

    def ready(self, service): return service != 'engine' or self.engine_ready


def wait(predicate, seconds, reason):
    if not predicate(): raise TimeoutError(reason)


def test_failed_engine_restore_does_not_restart_worker_and_always_restores_watchdog():
    manager = Manager(engine_ready=False)
    events = []
    failures = restore_services(manager, 'engine', 'worker', 'watchdog', wait, events.append)
    assert failures == ['TimeoutError']
    assert manager.actions == [('engine', 'start'), ('watchdog', 'start')]
    assert events == ['serving_pair_restore_failed', 'watchdog_restored']


def test_recovery_starts_worker_only_after_engine_readiness():
    manager = Manager()
    assert restore_services(manager, 'engine', 'worker', 'watchdog', wait, lambda event: None) == []
    assert manager.actions == [('engine', 'start'), ('worker', 'start'), ('watchdog', 'start')]
