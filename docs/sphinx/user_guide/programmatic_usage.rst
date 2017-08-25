Programmatic Usage
==================
You can use Jobman in your own programs.

This kind of usage is useful for things like:

#. Making custom job submission daemons.
#. Running Jobman itself in parallel on a cluster.
#. Integrating Jobman with an external workflow system.

=============
Configuration
=============
Configuration is important for programmatic usage. For example, configuration
ensures that every external program that accesses Jobman is using the same
databases and directories.

Jobman provides a utility to create a JobMan instance from a config:

The config can be a python module, a dictionary, or a python object. Jobman
will try to look up config parameters as dictionary keys or attributes, per
:meth:`jobman.utils.dot_spec_loader.get_attrs_or_items`.

Example:

.. testcode:: from_cfg_test_group

   from jobman.jobman import JobMan
   my_cfg = {
       'label': 'my_label',
       'db_uri': 'sqlite://',
       'auto_initialize': False,
   }
   my_jobman = JobMan.from_cfg(
       cfg=my_cfg,
       overrides={'label': 'label_override'}
   )
   print("label:", my_jobman.label)
   print("db_uri:", my_jobman.db_uri)

.. testoutput:: from_cfg_test_group

   label: label_override
   db_uri: sqlite://

Config parameters are documented in :class:`jobman.jobman.CfgParams`.

===============
Submitting Jobs
===============
There are two primary ways to submit jobs directly to JobMan:

#. As job_specs, via :meth:`jobman.jobman.JobMan.submit_job_spec`
#. As job dirs, via :meth:`jobman.jobman.JobMan.submit_job_dir`

See the documentation for those methods for more details.


=============
Querying Jobs
=============
You may want to query JobMan for the state of a job.

The primary methods for querying are:

#. :meth:`jobman.dao.jobman_sqlite_dao.JobmanSqliteDAO.get_job`
#. :meth:`jobman.dao.jobs_dao_mixin.JobsDaoMixin.query_jobs`

See the documentation for those methods for more details.

=============
Updating Jobs
=============
