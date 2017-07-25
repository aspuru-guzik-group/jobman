import json
from pathlib import Path
import tempfile
import unittest

from .. import constants
from .. import jobman
from ..dao import sqlite_dao
from .mock_engine import MockEngine


class IngestTestCase(unittest.TestCase):
    def setUp(self):
        self.dao = sqlite_dao.SqliteDAO()
        self.engine = MockEngine()
        self.source_dir = tempfile.mkdtemp(prefix='source_1.')
        self.source_key = 'my_source_key'
        self.source_cfg = {
            'type': 'dir',
            'root': self.source_dir,
            'default_job_spec': {
                'funko': 'barg'
            }
        }
        self.jobman = jobman.JobMan(
            dao=self.dao,
            engine=self.engine,
            job_engine_states_ttl=10,
            source_cfgs={
                self.source_key: self.source_cfg
            }
        )
        self.inbox_path = Path(self.source_cfg['root'], 'inbox')
        self.inbox_path.mkdir()
        self.queued_path = Path(self.source_cfg['root'], 'queued')
        self.job_specs = [{'spec_#': i} for i in range(3)]
        self.job_dirs = [self._generate_job_dir(job_spec=job_spec)
                         for job_spec in self.job_specs]
        self.jobman.tick()

    def _generate_job_dir(self, job_spec=None):
        job_spec = job_spec or {}
        job_dir = tempfile.mkdtemp(dir=self.inbox_path)
        job_spec_path = Path(job_dir) / constants.JOB_SPEC_FILE_NAME
        with open(job_spec_path, 'w') as f:
            json.dump(job_spec, f)
        return job_dir

    def test_moves_dirs_to_queued_dir(self):
        self.assertEqual(len(list(self.inbox_path.glob('*'))), 0)
        self.assertEqual(
            sorted(list(self.queued_path.glob('*'))),
            sorted([self.queued_path / Path(job_dir).name
                    for job_dir in self.job_dirs])
        )

    def test_creates_records_with_expected_attrs(self):
        expected_plucked_jobs = [
            {
                'source_key': self.source_key,
                'job_spec': {
                    'dir': str(self.queued_path / Path(job_dir).name),
                    **self.jobman.default_job_spec,
                    **self.source_cfg.get('default_job_spec', {}),
                    **self.job_specs[i]
                }
            }
            for i, job_dir in enumerate(self.job_dirs)
        ]
        actual_plucked_jobs = [
            self._pluck(job, keys=['source_key', 'job_spec'])
            for job in self.jobman.get_jobs()
        ]
        self.assertEqual(
            self._sort_jobs_by_dir(actual_plucked_jobs),
            self._sort_jobs_by_dir(expected_plucked_jobs)
        )

    def _pluck(self, dict_=None, keys=None):
        return {k: v for k, v in dict_.items() if k in keys}

    def _sort_jobs_by_dir(self, jobs=None):
        def sort_key_fn(j):
            return j['job_spec']['dir']
        return sorted(jobs, key=sort_key_fn)
