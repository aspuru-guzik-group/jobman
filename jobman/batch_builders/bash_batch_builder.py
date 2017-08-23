import textwrap

from jobman.constants import CHECKPOINT_FILE_NAMES
from .base_batch_builder import BaseBatchBuilder


class BashBatchBuilder(BaseBatchBuilder):
    class CfgSpecAggregationError(Exception):
        pass

    class CfgSpecMergeError(Exception):
        pass

    class InvalidPreambleError(Exception):
        def __init__(self, msg=None, preamble=None):
            msg = msg or ''
            hr = '-' * 10
            msg += "\n".join(["Preamble was:", hr, preamble, hr])
            super().__init__(msg)

    DEFAULT_PREAMBLE = 'PARALLEL=${PARALLEL:-/bin/bash}'

    def __init__(self, *args, default_preamble=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_preamble = default_preamble or self.DEFAULT_PREAMBLE

    def _get_preamble_errors(self, preamble=None):
        errors = []
        if 'PARALLEL=' not in preamble:
            errors.append("A line like 'PARALLEL=<your parallel cmd>' must be"
                          " present in preamble. It will be called like"
                          " '$PARALLEL < commands' .")
        return errors

    def _build_batch_jobdir(self, preamble=None):
        self._write_subjob_commands()
        self._write_entrypoint(preamble=preamble)
        job_spec = {
            'cfg_specs': {
                'PARALLEL': {'required': False, 'output_key': 'PARALLEL'},
                **self._get_merged_subjob_cfg_specs(),
            },
            'dir': str(self.jobdir_path),
            'entrypoint': str(self.entrypoint_path),
            'std_log_file_names': self.std_log_file_names,
        }
        return job_spec

    def _write_subjob_commands(self):
        with self.subjob_commands_path.open('w') as f:
            f.write(self._generate_subjob_commands_content())

    @property
    def subjob_commands_path(self):
        return (self.jobdir_path / 'subjob_commands.sh')

    def _generate_subjob_commands_content(self):
        subjob_commands = []
        for subjob in self.subjobs:
            subjob_commands.append(self._generate_subjob_command(
                subjob=subjob))
        return "\n".join(subjob_commands)

    def _generate_subjob_command(self, subjob=None):
        return (
            "pushd {dir} > /dev/null;"
            " if {entrypoint}; then touch {completed_checkpoint};"
            " else touch {failed_checkpoint};"
            " fi;"
            " popd > /dev/null"
        ).format(
            dir=subjob['job_spec']['dir'],
            entrypoint=subjob['job_spec']['entrypoint'],
            completed_checkpoint=CHECKPOINT_FILE_NAMES['completed'],
            failed_checkpoint=CHECKPOINT_FILE_NAMES['failed']
        )

    def _write_entrypoint(self, preamble=None):
        with self.entrypoint_path.open('w') as f:
            f.write(self._generate_entrypoint_content(preamble=preamble))
        self.entrypoint_path.chmod(0o755)

    def _generate_entrypoint_content(self, preamble=None):
        preamble = preamble or self._get_preamble()
        self._validate_preamble(preamble=preamble)
        return textwrap.dedent(
            """
            #!/bin/bash

            # parallel preamble
            {preamble}

            # parallel command
            $PARALLEL < {commands_file}
            """
        ).lstrip().format(
            preamble=(preamble or ''),
            commands_file=str(self.subjob_commands_path)
        )

    def _get_preamble(self):
        try:
            return self._generate_preamble()
        except NotImplementedError:
            return self.default_preamble

    def _generate_preamble(self): raise NotImplementedError

    def _validate_preamble(self, preamble=None):
        errors = self._get_preamble_errors(preamble=preamble)
        if errors:
            raise self.InvalidPreambleError(preamble=preamble,
                                            msg="\n".join(errors))

    def _get_merged_subjob_cfg_specs(self):
        merged_specs = {}
        for subjob in self.subjobs:
            subjob_cfg_specs = subjob['job_spec'].get('cfg_specs', {})
            for cfg_key, cfg_spec in subjob_cfg_specs.items():
                if cfg_key in merged_specs:
                    try:
                        cfg_spec = self._merge_cfg_specs(merged_specs[cfg_key],
                                                         cfg_spec)
                    except self.CfgSpecMergeError as exc:
                        error = ("Could not merge cfg_specs for cfg_key"
                                 " '{cfg_key}'").format(cfg_key=cfg_key)
                        raise self.CfgSpecAggregationError(error) from exc
                merged_specs[cfg_key] = cfg_spec
        return merged_specs

    def _merge_cfg_specs(self, *cfg_specs):
        merged = {}
        for cfg_spec in cfg_specs:
            merged['required'] = \
                    merged.get('required') or cfg_spec.get('required')
            if 'default' in cfg_spec:
                if 'default' in merged:
                    if merged['default'] != cfg_spec['default']:
                        error = ("Competing default values:"
                                 " '{}' and '{}'").format(merged['default'],
                                                          cfg_spec['default'])
                        raise self.CfgSpecMergeError(error)
                merged['default'] = cfg_spec['default']
        return merged
