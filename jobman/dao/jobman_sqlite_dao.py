from .jobs_dao_mixin import JobsDaoMixin
from .sqlite_dao import SqliteDAO
from . import utils as _dao_utils


class JobmanSqliteDAO(JobsDaoMixin, SqliteDAO):
    """DAO for JobMan"""
    def __init__(self, lock_timeout=30, debug=None, **kwargs):
        self.debug = debug
        self.lock_timeout = lock_timeout
        super().__init__(
            orm_specs=self._generate_orm_specs(),
            table_prefix='jobman_',
            **kwargs
        )

    class JobSchema(_dao_utils.TimestampedSchemaMixin, _dao_utils.Schema):
        """Fields for job records."""
        key = {'type': 'TEXT', 'primary_key': True,
               'default': _dao_utils.generate_key}
        job_spec = {'type': 'JSON'}
        """dict of job metadata/parameters.

        A job_spec often includes these items:

            batchable
                Whether a job can be included in a batch.
            dir
                Absolute path to job_dir.
            cfg
                Extra cfg values to provide to cfg resolvers.
            resources
                Resources job requires.
        """

        status = {'type': 'TEXT'}
        batchable = {'type': 'INTEGER'}
        """Flag to indicate whether a job can be included in a batch."""
        worker_key = {'type': 'TEXT'}
        """Key for worker handling the job."""
        worker_meta = {'type': 'JSON'}
        """Metadata to retrieve related state from worker."""
        errors = {'type': 'JSON'}
        source_key = {'type': 'TEXT'}
        """Key for source that provided this job."""
        source_meta = {'type': 'JSON'}
        """Metadata from source, to allow source to correlated jobman records
        with its own records."""
        source_tag = {'type': 'TEXT'}
        """A tag that the source can set. This is often useful for sources
        which want to do some sort of post-processing before marking a job
        as complete."""
        purgeable = {'type': 'INTEGER'}
        """Whether a job can be purged from the db."""

    def _generate_orm_specs(self):
        return [{'name': 'job', 'fields': self.JobSchema.get_field_infos()}]

    def generate_source_key_filter(self, source_key=None):
        return {'field': 'source_key', 'op': '=', 'arg': source_key}
