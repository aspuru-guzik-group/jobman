#!/bin/bash

# start preamble
# engine_preamble

# env_vars_for_cfg_specs

# end preamble

pushd "/Users/adorsk/projects/jobman/job_dirs/queued/my_job_dir" > /dev/null && ./entrypoint.sh
RESULT=$?
if [ $RESULT -eq 0 ]; then
    touch JOBMAN-COMPLETED
else
    touch JOBMAN-FAILED
fi
popd > /dev/null
exit $RESULT
