from collections import defaultdict
import unittest
from unittest.mock import call, MagicMock

from .. import bash_batch_builder


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.builder = bash_batch_builder.BashBatchBuilder()
        self.builder.batch_job = self.generate_mock_job()
        self.builder.subjobs = [self.generate_mock_job() for i in range(3)]
        self.builder.dest = MagicMock()

    def generate_mock_job(self):
        return defaultdict(MagicMock)

    def mockify_builder_attrs(self, attrs=None):
        for attr in attrs: setattr(self.builder, attr, MagicMock())

class _BuildBatchJobdirTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.mockify_builder_attrs(attrs=['_get_merged_subjob_cfg_specs',
                                          '_write_subjob_commands',
                                          '_write_entrypoint'])
        self.preamble = MagicMock()
        self.result = self.builder._build_batch_jobdir(preamble=self.preamble)

    def test_writes_subjob_commands(self):
        self.assertEqual(self.builder._write_subjob_commands.call_args, call())

    def test_writes_entrypoint(self):
        self.assertEqual(self.builder._write_entrypoint.call_args,
                         call(preamble=self.preamble))

    def test_gets_merged_subjob_cfg_specs(self):
        self.assertEqual(
            self.builder._get_merged_subjob_cfg_specs.call_args, call())

    def test_returns_expected_job_spec(self):
        expected_job_spec = {
            'cfg_specs': (self.builder._get_merged_subjob_cfg_specs
                          .return_value),
            'dir': self.builder.jobdir,
            'entrypoint': self.builder.entrypoint_path,
            'std_log_file_names': self.builder.std_log_file_names,
        }
        self.assertEqual(self.result, expected_job_spec)

class _GetMergedSubJobCfgSpecsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.subjob_cfg_specs_list = [MagicMock() for i in range(3)]

    def _get_merged_subjob_cfg_specs(self, subjob_cfg_specs_list=None):
        subjob_cfg_specs_list = subjob_cfg_specs_list or \
                self.subjob_cfg_specs_list
        self.builder.subjobs = []
        for subjob_cfg_specs in self.subjob_cfg_specs_list:
            subjob = self.generate_mock_job()
            subjob['job_spec'] = {'cfg_specs': subjob_cfg_specs}
            self.builder.subjobs.append(subjob)
        return self.builder._get_merged_subjob_cfg_specs()

    def test_raises_if_cfg_specs_are_not_mergable(self):
        self.subjob_cfg_specs_list = [
            {'cfg_key1': {'default': 'value1'}},
            {'cfg_key1': {'default': 'value2'}},
        ]
        with self.assertRaises(self.builder.CfgSpecAggregationError):
            self._get_merged_subjob_cfg_specs()

    def test_returns_merged_cfg_specs(self):
        self.subjob_cfg_specs_list = [
            {
                'key1': {'required': False, 'default': 'key1_default'}
            },
            {
                'key1': {'required': True},
                'key2': {'required': False},
                'key3': {'default': 'key3_default'},
            },
            {
                'key3': {'required': True, 'default': 'key3_default'},
            }
        ]
        expected_result = {
            'key1': {'required': True, 'default': 'key1_default'},
            'key2': {'required': False},
            'key3': {'required': True, 'default': 'key3_default'},
        }
        actual_result = self._get_merged_subjob_cfg_specs()
        self.assertEqual(actual_result, expected_result)
