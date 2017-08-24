import textwrap


def generate_default_config_content():
    return textwrap.dedent(
        '''
        import os


        this_dir = os.path.dirname(__file__)

        label = 'my_jobman'

        db_file = os.path.join(this_dir, 'jobman.db.sqlite')
        jobman_db_uri = 'sqlite:///' + db_file

        source_specs = {
            'my_dir_source': {
                'source_class': 'jobman.sources.dir_source:DirSource',
                'source_params': {
                    'root_path': os.path.join(this_dir, 'job_dirs')
                }
            }
        }

        worker_specs = {
            'local_worker': {
                'worker_params': {
                    'engine_spec': {
                        'engine_class': (
                            'jobman.engines.local_engine:LocalEngine'
                        ),
                        'engine_params': {
                            'cfg': {
                                'SOME_ENV_VAR': 'SOME_VALUE'
                            }
                        }
                    }
                }
            }
        }
        '''
    ).lstrip()
