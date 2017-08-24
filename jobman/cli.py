import argparse
import datetime
import inspect
import time


def main(args=None):
    Cli().run(args=args)


class Cli(object):
    class UnrecognizedCommandError(Exception):
        def __init__(self, msg=None, command=None, **kwargs):
            msg = msg or ''
            if command:
                if msg: msg += '; '  # noqa
                msg += ("Command was '{command}'".format(command=command))
            super().__init__(msg, **kwargs)

    def run(self, args=None):
        root_parser = self._generate_root_argument_parser()
        parsed, unparsed = self._parse_known_args(root_parser, args[:1])
        if parsed['command']:
            self._handle_command(
                command=parsed['command'],
                unparsed_args=args[1:]
            )

    def _generate_root_argument_parser(self):
        root_parser = self._generate_argument_parser()
        command_infos = self._get_command_infos()
        root_parser.add_argument(
            'command',
            help=self._get_command_help(command_infos=command_infos),
        )
        return root_parser

    def _generate_argument_parser(self, **kwargs):
        return argparse.ArgumentParser(**{
            'formatter_class': argparse.RawTextHelpFormatter,
            **kwargs
        })

    def _parse_known_args(self, parser=None, args=None):
        parsed, unparsed = parser.parse_known_args(args=args)
        return (vars(parsed), unparsed)

    def _get_command_infos(self):
        command_infos = []
        method_members = inspect.getmembers(self, predicate=inspect.ismethod)
        for name, method in method_members:
            if name.startswith('_handle_') and name != '_handle_command':
                command_infos.append({
                    'name': name.replace('_handle_', ''),
                    'description': getattr(method, '__doc__', None),
                })
        return sorted(command_infos, key=(lambda info: info['name']))

    def _get_command_help(self, command_infos=None):
        command_infos = command_infos or self._get_command_infos()
        return 'Command must be one of:\n{command_infos}'.format(
            command_infos="\n".join([
                '{name} | {description}'.format(**info)
                for info in command_infos
            ])
        )

    def _handle_command(self, command=None, unparsed_args=None):
        handler_fn_name = '_handle_%s' % command
        if not hasattr(self, handler_fn_name):
            raise self.UnrecognizedCommandError(
                msg=("Command must be one of: [{choices}]").format(
                    choices=(', '.join([
                        info['name'] for info in self._get_command_infos()
                    ]))
                ),
                command=command
            )
        getattr(self, handler_fn_name)(unparsed_args=unparsed_args)

    def _handle_sanity_check(self, unparsed_args=None):
        """Sanity check."""
        print("Hello Jobman!")

    def _handle_generate_default_config(self, unparsed_args=None):
        """Generate a default config file."""
        from jobman.utils import config_utils
        print(config_utils.generate_default_config_content())

    def _handle_generate_echo_job_dir(self, unparsed_args=None):
        """Generate a job_dir that runs an echo command."""
        parser = self._generate_argument_parser()
        parser.add_argument(
            '--job_dir_path',
            help='Path at which to create job_dir. If unspecified uses tmpdir',
        )
        parser.add_argument(
            '--message',
            help='message to echo in job',
            default='some message'
        )
        parsed_args, unparsed_args = parser.parse_known_args(unparsed_args)
        parsed_args = vars(parsed_args)
        from jobman.utils import testing_utils
        print(parsed_args['message'])
        result = testing_utils.generate_echo_job_dir(**parsed_args)
        print("Created job_dir at '{job_dir}'".format(
            job_dir=result['job_dir']))

    def _handle_initialize(self, unparsed_args=None):
        """Initialize a source."""
        parser = self._generate_argument_parser()
        self._add_cfg_argument(parser)
        parsed, unparsed = self._parse_known_args(parser, unparsed_args)
        self._generate_jobman(
            cfg_file_path=parsed['cfg'],
            jobman_kwargs={'initialize': True}
        )
        print("Initialized")

    def _add_cfg_argument(self, parser=None, pos_args=('-c', '--cfg'),
                          argument_kwargs=None):
        kwargs = {
            'help': 'path to jobman cfg python file',
            'required': True,
            **(argument_kwargs or {})
        }
        parser.add_argument(*pos_args, **kwargs)

    def _generate_jobman(self, cfg_file_path=None, jobman_kwargs=None):
        cfg_file_path = cfg_file_path or self.parsed_args['cfg']
        from jobman.jobman import JobMan
        from jobman.utils import import_utils
        cfg = import_utils.load_module_from_path(cfg_file_path)
        return JobMan.from_cfg(cfg=cfg, overrides=jobman_kwargs)

    def _handle_tick(self, unparsed_args=None):
        """Run Jobman ticks"""
        parser = self._generate_argument_parser()
        self._add_cfg_argument(parser)
        parser.add_argument(
            '--nticks',
            help='number of ticks to run',
            type=int,
            default=1
        )
        parser.add_argument(
            '--tick_interval',
            help='time in seconds to wait between ticks',
            type=float,
            default=120
        )
        parsed, unparsed = self._parse_known_args(parser, unparsed_args)
        jobman = self._generate_jobman(
            cfg_file_path=parsed['cfg'],
            jobman_kwargs={'initialize': True}
        )
        nticks, tick_interval = parsed['nticks'], parsed['tick_interval']
        tick_counter = 0
        while True:
            tick_counter += 1
            jobman.logger.info('Tick #{tick_counter}'.format(
                tick_counter=tick_counter))
            jobman.tick()
            jobman.logger.info('Next tick at {next_tick_time}'.format(
                next_tick_time=(
                    (
                        datetime.datetime.now()
                        + datetime.timedelta(seconds=tick_interval)
                    )
                )
            ))
            if nticks and tick_counter >= nticks:
                break
            else:
                time.sleep(tick_interval)


if __name__ == '__main__':
    main()
