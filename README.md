# Kong ![CI](https://github.com/paulgessinger/kong/workflows/CI/badge.svg) [![codecov](https://codecov.io/gh/paulgessinger/kong/branch/master/graph/badge.svg)](https://codecov.io/gh/paulgessinger/kong) [![pypi](https://img.shields.io/pypi/v/kong-batch)](https://pypi.org/project/kong-batch/) [![docs](https://readthedocs.org/projects/kong-batch/badge/?version=latest)](https://kong-batch.readthedocs.io)
[Documentation](https://kong-batch.readthedocs.io)



## What does this do?
Suppose you use a batch cluster somewhere to run parallel workloads. Normally
you'd write dedicated submission code for each type of system and use the
relevant shell commands to monitor job progress. How do you keep track
of what happened to jobs? How do you even keep track of which job did what?

With kong, you can organize you jobs into *folders* (not actual folders on disk),
however you feel like it. Kong can keep track of job statuses and reports
them to you in a clean and organized view. You can manage your jobs 
in kong, kill them, resubmit them, remove them. Kong also normalizes things
like where the your job can find scratch space, where to put log files
and where to put output files. This is done by a set of environment variables
available in every job, regardless of backend (called *driver*):

variable name       | value
--------------------|------
KONG_JOB_ID         | Kong-specific job ID (not the batch system one)
KONG_JOB_OUTPUT_DIR | Where to put output files
KONG_JOB_LOG_DIR    | Where log files go
KONG_JOB_NPROC      | How many core your job can use
KONG_JOB_SCRATCHDIR | scratch dir for the job

You can write job scripts that are mostly agnostic to which driver is
used to execute the job. Some things remain specific to your environment,
especially things that are implemented on top of the actual batch system. This
includes things like licenses, queue names, and any other specific configuration.
Kong allows you to provide arguments like this either via configuration, or on
a job by job basis.

## Interface

### REPL

Kong provides a command line like program. If you run

[![asciicast](https://asciinema.org/a/hnBQ7S4GQQj2uGI42kbOQyHw4.svg)](https://asciinema.org/a/hnBQ7S4GQQj2uGI42kbOQyHw4)


