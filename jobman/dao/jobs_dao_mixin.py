import time

from jobman import constants


class JobsDaoMixin(object):
    """Common methods used by daos that work with jobs."""

    JOB_STATUSES = constants.JOB_STATUSES

    def create_job(self, job=None):
        return self.create_ent(ent_type='job', ent=job)

    def save_jobs(self, jobs=None, replace=True):
        return self.save_ents(ent_type='job', ents=jobs, replace=replace)

    def query_jobs(self, query=None):
        return self.query_ents(ent_type='job', query=query)

    def update_jobs(self, updates=None, query=None):
        return self.update_ents(ent_type='job', updates=updates, query=query)

    def claim_jobs(self, query=None):
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
        return self.get_ents(ent_type='job', key=key)

    def get_jobs_for_status(self, status=None):
        return self.query_jobs(query={
            'filters': [self.generate_status_filter(status=status)]
        })

    def generate_status_filter(self, status=None):
        return {'field': 'status', 'op': '=', 'arg': status}
