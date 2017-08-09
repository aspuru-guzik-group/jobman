class BaseJobSource(object):
    def __init__(self, *args, key=None, jobman=None, **kwargs):
        self.key = key
        self.jobman = jobman

    def tick(self): raise NotImplementedError()

    def query_jobs(self, query=None):
        query = query or {}
        query = {
            **query,
            'filters': [
                self.jobman.generate_source_key_filter(source_key=self.key),
                *(query.get('filters') or [])
            ]
        }
        return self.jobman.query_jobs(query=query)

    def get_jobs_for_status(self, status=None):
        return self.get_jobs(query={
            'filters': [self.jobman._generate_status_filter(status=status)]
        })
