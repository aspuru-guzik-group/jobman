import os
import textwrap

from .base_batch_jobdir_builder import BaseBatchJobdirBuilder


class BashBatchJobdirBuilder(BaseBatchJobdirBuilder):
    def __init__(self, *args, run_commands_command='bash', **kwargs):
        super().__init__(*args, **kwargs)
        self.run_commands_command = run_commands_command
        self.subjob_commands_path = os.path.join(self.jobdir, 'subjob_commands')

    def _build_batch_jobdir(self):
        self._write_subjob_commands()
        self._write_entrypoint()
        jobdir_meta = {
            'dir': self.jobdir,
            'entrypoint': self.entrypoint_path,
            'std_log_file_names': self.std_log_file_names,
        }
        return jobdir_meta

    def _write_subjob_commands(self):
        with open(self.subjob_commands_path, 'w') as f:
            f.write(self._generate_subjob_commands_content())

    def _generate_subjob_commands_content(self):
        subjob_commands = []
        for subjob in self.subjobs:
            subjob_commands.append(self._generate_subjob_command(subjob=subjob))
        return "\n".join(subjob_commands)

    def _generate_subjob_command(self, subjob=None):
        return "pushd {dir}; {entrypoint}; popd".format(
            dir=subjob['jobdir_meta']['dir'],
            entrypoint=os.path.join(subjob['jobdir_meta']['dir'],
                                    subjob['jobdir_meta']['entrypoint'])
        )

    def _write_entrypoint(self):
        with open(self.entrypoint_path, 'w') as f:
            f.write(self._generate_entrypoint_content())
        os.chmod(self.entrypoint_path, 0o755)

    def _generate_entrypoint_content(self):
        return textwrap.dedent(
            """
            #!/bin/bash
            {run_commands_command} {commands_file}
            """
        ).lstrip().format(
            run_commands_command=self.run_commands_command,
            commands_file=self.subjob_commands_path
        )

