import time

from jobman import constants


class JobsDaoMixin(object):
    """Common methods used by daos that work with jobs."""

    JOB_STATUSES = constants.JOB_STATUSES

    def create_job(self, job=None):
        """Create a job record.

        Args:
            job: a job dict. Format of dict depends on schema for DAO.

        Returns:
            job record from DAO.
        """
        return self.create_ent(ent_type='job', ent=job)

    def save_jobs(self, jobs=None, replace=True):
        """Save jobs to DAO.

        Args:
            jobs: A list of job dicts.
            replace: If True will overrwrite existing records. If False will
                raise errors for key collisions.

        Returns:
            saved jobs.
        """
        return self.save_ents(ent_type='job', ents=jobs, replace=replace)

    def query_jobs(self, query=None):
        """query DAO for jobs.

        Args:
            query: A query dict per
                :meth:`jobman.dao.sqlite_dao.SqliteDAO.query_ents`.

        Returns:
            a list of job records matching query.
        """
        return self.query_ents(ent_type='job', query=query)

    def update_jobs(self, updates=None, query=None):
        """Update jobs which match a query.

        Args:
            query: A query dict per :meth:`query_jobs`.

        Returns:
            a dict with a 'rowcount' item indicating how many records were
                updated.
        """
        return self.update_ents(ent_type='job', updates=updates, query=query)

    def claim_jobs(self, query=None):
        """Claim jobs which match a query.

        Sets status to :attr:`JOB_STATUSES.CLAIMED`.

        Args:
            query: A query dict per :meth:`query_jobs`.

        Returns:
            a list of claimed jobs.
        """
        now = time.time()
        jobs_to_claim = self.query_jobs(query=query)
        job_keys = [job['key'] for job in jobs_to_claim]
        self.update_jobs(
            updates={
                'status': self.JOB_STATUSES.CLAIMED,
                'modified': now
            },
            query={
                'filters': [
                    {'field': 'modified', 'op': '<=', 'arg': now},
                    {'field': 'key', 'op': 'IN', 'arg': job_keys}
                ],
            }
        )
        claimed_jobs = self.query_jobs(query={
            'filters': [
                self.generate_status_filter(status=self.JOB_STATUSES.CLAIMED),
                {'field': 'key', 'op': 'IN', 'arg': job_keys}
            ]
        })
        return claimed_jobs

    def get_job(self, key=None):
        """Get a job that has the given key.

        Args:
            key: job key

        Returns:
            job_record
        """
        return self.get_ents(ent_type='job', key=key)

    def get_jobs_for_status(self, status=None):
        """Get jobs that have a given status.

        Args:
            status: job status

        Returns:
            job_records
        """
        return self.query_jobs(query={
            'filters': [self.generate_status_filter(status=status)]
        })

    def generate_status_filter(self, status=None):
        """Generate a status filter.

        Args:
            status: job status

        Returns:
            status_filter_dict
        """
        return {'field': 'status', 'op': '=', 'arg': status}
