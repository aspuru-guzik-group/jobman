JOB_SPEC_FILE_NAME = 'JOBMAN-JOB_SPEC.json'
CHECKPOINT_FILE_NAMES = {
    'completed': 'JOBMAN-COMPLETED',
    'failed': 'JOBMAN-FAILED',
}


class JOB_STATUSES(object):
    CLAIMED = 'CLAIMED'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    EXECUTED = 'EXECUTED'
    FAILED = 'FAILED'
    UNKNOWN = 'UNKNOWN'
    COMPLETED = 'COMPLETED'
