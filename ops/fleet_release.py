"""Fleet release ordering and failure recovery, independent of service I/O.

No CLI or background controller is installed by this module. The caller must
bind each operation to exact managed identities, original configurations and
the existing exclusive campaign lock. Engine/model replacement is excluded.
"""
from __future__ import annotations


class ReleaseFailed(RuntimeError):
    def __init__(self,report):
        self.report=report
        super().__init__('Fleet release did not complete; inspect the fixed-stage report')


def require_closed_reuse_campaign(state,report):
    count=report.get('configuration',{}).get('tasks')
    reuse=report.get('input_reuse',{})
    if (state.get('phase')!='complete' or count!=3600 or report.get('passed') is not True
        or report.get('accepted_tasks')!=count or report.get('completed_tasks')!=count
        or report.get('unfinished_tasks')!=0 or report.get('failed_tasks')!=0
        or report.get('accepted_exactly_once') is not True
        or report.get('accepted_usage_matches_task_usage') is not True
        or report.get('fixed_output_lengths_exact') is not True
        or reuse.get('enabled') is not True or reuse.get('new_upload_requests')!=0):
        raise RuntimeError('Successful closed 3600-task reuse evidence required')


def release_sequence(*,preflight,watchdog,pause,drain,install_worker,validate_worker,
                     install_apis,validate_apis,reopen,pause_compatible_apis,
                     ensure_draining_worker,restore_worker,validate_original,
                     reopen_original,event):
    """Close both gates and drain again before a rollback can stop a worker.

    ``drain`` must include durable pending/running tasks, worker inflight and
    engine quiescence. ``pause_compatible_apis`` installs known-compatible old
    API configurations with enqueue disabled, even if candidate APIs failed.
    ``ensure_draining_worker`` may start a known compatible worker if the owned
    candidate is already dead; it must not kill a live worker with accepted work.
    No recovery stage reopens admission before old readiness is established.
    """
    report={'passed':False,'events':[],'rollback_attempted':False,'rollback_passed':False,
            'watchdog_recovery_attempted':False,'engine_actions':False}
    stage='preflight';mutation_started=False;watchdog_attempted=False
    def perform(name,action,*,recovering=False):
        nonlocal stage
        stage=name;report['events'].append(name)
        try:event(name)
        except BaseException as logging:
            report.setdefault('journal_errors',[]).append({'stage':name,'error_type':type(logging).__name__})
            report['passed']=False
            report.setdefault('failed_stage',name);report.setdefault('error_type',type(logging).__name__)
            if not recovering:raise
        action()
    try:
        perform('preflight',preflight)
        watchdog_attempted=True;perform('watchdog_off',lambda:watchdog(False))
        mutation_started=True
        perform('closing_admission',pause)
        perform('draining',drain)
        perform('installing_worker',install_worker)
        perform('validating_worker',validate_worker)
        perform('installing_apis',install_apis)
        perform('validating_apis',validate_apis)
        perform('reopening_admission',reopen)
        perform('validating_open_primary',validate_apis)
        report['passed']=True
    except BaseException as exc:
        report.update(failed_stage=stage,error_type=type(exc).__name__)
        if mutation_started:
            report['rollback_attempted']=True
            try:
                perform('rollback_closing_compatible_apis',pause_compatible_apis,recovering=True)
                perform('rollback_ensuring_draining_worker',ensure_draining_worker,recovering=True)
                perform('rollback_draining_accepted_work',drain,recovering=True)
                perform('rollback_restoring_worker',restore_worker,recovering=True)
                perform('rollback_validating_original',validate_original,recovering=True)
                perform('rollback_reopening_original',reopen_original,recovering=True)
                perform('rollback_validating_open_original',validate_original,recovering=True)
                report['rollback_passed']=True
            except BaseException as recovery:
                report.update(rollback_failed_stage=stage,rollback_error_type=type(recovery).__name__)
                # If reopen had started before a failed validation, close it
                # again. Never claim closed gates if that operation fails.
                try:
                    perform('rollback_leaving_admission_closed',pause_compatible_apis,recovering=True)
                    report['admission_left_closed']=True
                except BaseException as closing:
                    report.update(admission_left_closed=False,admission_close_error_type=type(closing).__name__)
    finally:
        if watchdog_attempted:
            report['watchdog_recovery_attempted']=True
            try:perform('watchdog_on',lambda:watchdog(True),recovering=True)
            except BaseException as exc:
                report.update(passed=False,watchdog_error_type=type(exc).__name__)
    if not report['passed']:raise ReleaseFailed(report) from None
    return report
