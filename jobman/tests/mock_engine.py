import uuid


class MockEngine(object):
    def __init__(self):
        self.jobs = {}

    def submit_job(self, job=None, extra_cfgs=None):
        engine_meta = str(uuid.uuid4())
        self.jobs[engine_meta] = {'status': 'RUNNING'}
        return engine_meta

    def get_keyed_engine_states(self, keyed_engine_metas=None):
        return {
            key: self.jobs.get(engine_meta)
            for key, engine_meta in keyed_engine_metas.items()
        }

    def complete_job(self, engine_meta=None):
        self.jobs[engine_meta]['status'] = 'EXECUTED'

    def unregister_job(self, engine_meta=None):
        del self.jobs[engine_meta]
