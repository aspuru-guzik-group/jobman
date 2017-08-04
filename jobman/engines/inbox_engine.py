from .base_engine import BaseEngine


class JobManInboxEngine(BaseEngine):
    def __init__(self, inbox_dir=None, **kwargs):
        super().__init__(**kwargs)
        self.inbox_dir = inbox_dir

    def submit_job(self, job=None, cfg=None):
        pass

    def get_keyed_engine_states(self, keyed_engine_metas=None):
        pass
