from jobman.utils import dot_spec_loader


class Worker(object):
    class IncompatibleJobError(Exception):
        pass

    def __init__(self, key=None, engine_spec=None, acceptance_fn_spec=None):
        self.key = key
        self.engine = self._engine_spec_to_engine(engine_spec)
        self.acceptance_fn = self._acceptance_fn_spec_to_acceptance_fn(
            acceptance_fn_spec)

    def _engine_spec_to_engine(self, engine_spec=None):
        engine_class = engine_spec['engine_class']
        if isinstance(engine_class, str):
            engine_class = dot_spec_loader.load_from_dot_spec(engine_class)
        engine = engine_class(**(engine_spec.get('engine_params') or {}))
        engine.key = self.key + '_engine'
        return engine

    def _acceptance_fn_spec_to_acceptance_fn(self, acceptance_fn_spec=None):
        if not acceptance_fn_spec:
            acceptance_fn = self.default_acceptance_fn
        if isinstance(acceptance_fn_spec, str):
            acceptance_fn = dot_spec_loader.load_from_dot_spec(
                acceptance_fn_spec)
        elif callable(acceptance_fn_spec):
            acceptance_fn = acceptance_fn_spec
        if not acceptance_fn:
            raise Exception("Could not parse acceptance_fn_spec")
        return acceptance_fn

    def default_acceptance_fn(self, job=None, **kwargs): return True

    def can_accept_job(self, job=None):
        try:
            return self.acceptance_fn(job=job)
        except Exception as exc:
            raise self.IncompatibleJobError() from exc

    def submit_job(self, job=None, extra_cfgs=None):
        return self.engine.submit_job(job=job, extra_cfgs=extra_cfgs)

    def get_keyed_states(self, keyed_metas=None):
        return self.engine.get_keyed_states(keyed_metas)

    def tick(self):
        if hasattr(self.engine, 'tick'): self.engine.tick()  # noqa
