class BaseSource(object):
    def __init__(self, *args, key=None, jobman=None, **kwargs):
        self.key = key
        self.jobman = jobman

    def tick(self): raise NotImplementedError()

    def query_jobs(self, query=None):
        query = query or {}
        query = {
            **query,
            'filters': [
                self.jobman.dao.generate_source_key_filter(
                    source_key=self.key),
                *(query.get('filters') or [])
            ]
        }
        return self.jobman.dao.query_jobs(query=query)
