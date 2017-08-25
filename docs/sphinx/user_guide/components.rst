Components
==========

Jobman is a collection of several components.

======
JobMan
======

The :class:`jobman.jobman.JobMan` class stores configuration, manages a central queue
of jobs, and orchestrates the other components.

=======
Sources
=======
Sources submit jobs to JobMan.

For example, :class:`jobman.sources.dir_source.DirSource` periodically scans
an inbox directory for new job_dirs. When it finds a new job_dir it submits it
to JobMan.

Sources are optional. You can extend JobMan by writing your own Source classes.

=======
Engines
=======
Engines act as adapters between JobMan and a cluster resource that executes a
job.

For example, :class:`jobman.engines.slurm_engine` creates a Slurm submission
for a Jobman job.

Often you want to provide an engine with specific configurations.
Some examples:

#. Paths to executables.
#. A preamble that activates a conda environment before every job.
#. Default memory and time parameters.

=======
Workers
=======
Workers act as adapters between JobMan and Engines. A worker's responsibilities
can include:

#. Managing the queue for an individual engine.
#. Determining whether an engine can meet a job's requirements.
#. Consolidating a large number of small jobs into a smaller number of batch
   jobs.

For example, :class:`jobman.workers.base_worker` provides basic queuing, and
:class:`jobman.workers.batching_worker` provides batching.

===
DAO
===
A `Data Access Object <https://en.wikipedia.org/wiki/Data_access_object>`_, or
DAO acts as interface to a persistence mechanism.

JobMan uses a DAO to abstract its underlying Sqlite storage. Normally users
don't need to interact with the DAO. But occasionally there may be a need to
query JobMan's DB. See :mod:`jobman.dao`.
