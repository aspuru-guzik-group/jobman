
.. title:: Jobman

======
Jobman
======
Jobman: A meta-scheduler for cluster computing.

======
Source
======

Main repo: |repo_url| .

========
Features
========

#. Ingest jobs from directories.
#. Distribute jobs to multiple workers.
#. Run jobs on local and cluster resources.

==========
Quickstart
==========

#. Ensure Requirements:

   .. code-block:: console 

      $ python --version
      # Should be >= 3

#. Install:

   Option A: pip install:

      .. parsed-literal ::

        pip install git+\ |repo_url|\ 

   Option B: git clone:

      #. Clone:

         .. parsed-literal ::
     
           git clone \ |repo_url|\  jobman_repo

      #. Make an alias to the standalone jobman script:

         .. code-block:: console

           $ alias jobman="$PWD/jobman_repo/standalone-jobman-entrypoint.sh"

#. Sanity check:

   .. code-block:: console

     $ jobman sanity_check
     Hello Jobman!

#. Generate default config:

   .. code-block:: console

     $ jobman generate_config > my_jobman_cfg.py

#. Initialize dbs & directories:

   .. code-block:: console

     $ jobman initialize
     Initialized.

#. Create a basic job dir:

   .. code-block:: console

     $ jobman generate_echo_job_dir \
       --job_dir_path="job_dirs/inbox/my_job_dir" \
       --message="my message!"
     Created job_dir at 'job_dirs/inbox/my_job_dir'

#. Run the job's entrypoint manually:

   .. code-block:: console

     $ job_dirs/inbox/my_job_dir/entrypoint.sh
     my message!

#. Run the job via Jobman:

   .. code-block:: console

     $ jobman --cfg=my_jobman_cfg.py tick --nticks=5
     $ find job_dirs -maxdepth 3
     job_dirs
     job_dirs/completed
     job_dirs/completed/my_job_dir
     job_dirs/completed/my_job_dir/entrypoint.sh
     job_dirs/completed/my_job_dir/job_spec.json
     job_dirs/completed/my_job_dir/JOBMAN-COMPLETED
     job_dirs/completed/my_job_dir/JOBMAN.ENTRYPOINT.sh
     job_dirs/completed/my_job_dir/stderr
     job_dirs/completed/my_job_dir/stdout
     job_dirs/failed
     job_dirs/inbox
     job_dirs/queued


#. Run Jobman as a Daemon:

   @TODO!

===============
Getting Started
===============

.. toctree::
   :maxdepth: 1

   user_guide/index.rst
   examples


=================
API Documentation
=================

.. toctree::
   :maxdepth: 1

   api

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
