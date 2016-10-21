# kong


## installation
- Clone the repository somewhere. 
- Add the folder `bin` to your `$PATH`
- Add the respository root to your `$PYTHONPATH`, if you want to use it from Python.
- Also, you need to clone my [python_utils](https://gitlab.cern.ch/pagessin/python_utils)
    - add it to your `$PYTHONPATH`


## usage from python
The module exposes a function called submit which looks like so:

```python
def submit(lists, config=None, dir=None, verbosity=None, dry_run=False):
    # ...
```
The only required argument lists needs to be a list of tuples with the format `(NAME, LISTFILE)`.

## usage from command line

- [peek](#peek)
- [submit](#submit)
- [ls](#ls)
- [rm](#rm)
- [kill](#kill)
- [recover](#recover)
- [resubmit](#resubmit)
- [view](#view)

```
usage: kong [-h] {peek,submit,resubmit,kill,ls,rm,recover,view} ...

kong submits jobs to LSF and keeps track of them, with a directory hierarchy
of .job files

positional arguments:
  {peek,submit,resubmit,kill,ls,rm,recover,view}
    peek                Show the stdout or stderr of a given subjob
    submit              Submits jobs to the batch system. Input is one or
                        multiple lists. Job files are created in the current
                        working directory.
    ls                  Show information over job files or directory
                        hierarchies with job files
    rm                  Remove a job or range of jobs. This removes the job
                        file, job stdout and stderr files as well as the
                        analysis output. You might want to kill the job(s)
                        first.
    kill                Kill a job or range of jobs. Note that it can take a
                        moment, before kong ls sees the status changes
    recover             Attempt to recover a lost jobs file from bjobs output
    resubmit            Requeue subjobs with a specified status back to the
                        batch system
    view                Show the subjobs and their statuses for a job range

optional arguments:
  -h, --help            show this help message and exit
``` 

### peek
```
usage: kong peek [-h] [--verbose] {out,err} jobid subjobid

positional arguments:
  {out,err}      Which stream to peek into
  jobid          The job id to peek into
  subjobid       The subjobid to peek into

optional arguments:
  -h, --help     show this help message and exit
  --verbose, -v  Increase verbosity
```

### submit
```
usage: kong submit [-h] [--verbose] [--name NAME] [--dry-run]
                   lists [lists ...]

positional arguments:
  lists          The lists to submit. You can glob in the shell

optional arguments:
  -h, --help     show this help message and exit
  --verbose, -v  Increase verbosity
  --name NAME    Name to assign to the job. Only works when a single list is
                 given
  --dry-run      Don't really do anything
```

### ls
```
usage: kong ls [-h] [--verbose] [--all] [--force] [dir]

positional arguments:
  dir            The directory or files to list. You can glob

optional arguments:
  -h, --help     show this help message and exit
  --verbose, -v  Increase verbosity
  --all, -a      If given, list all jobs found in the registry
  --force, -f    Kill the bjobs cache
```

### rm
```
usage: kong rm [-h] [--verbose] tgt [tgt ...]

positional arguments:
  tgt            Specify a job range. Can be '1 2 3', '1-3', '3+4' (this means
                 3, 4, 5, 6, 7), or job files or directories

optional arguments:
  -h, --help     show this help message and exit
  --verbose, -v  Increase verbosity
```

### kill
```
usage: kong kill [-h] [--verbose] tgt [tgt ...]

positional arguments:
  tgt            Specify a job range. Can be '1 2 3', '1-3', '3+4' (this means
                 3, 4, 5, 6, 7), or job files or directories

optional arguments:
  -h, --help     show this help message and exit
  --verbose, -v  Increase verbosity
```

### recover
```
usage: kong recover [-h] [--verbose] job

positional arguments:
  job            Job id, for which to attempt recovery

optional arguments:
  -h, --help     show this help message and exit
  --verbose, -v  Increase verbosity
```

### resubmit
```
usage: kong resubmit [-h] [--verbose] [--status {done,pend,exit,run}]
                     [--force]
                     tgt [tgt ...]

positional arguments:
  tgt                   Specify a job range. Can be '1 2 3', '1-3', '3+4'
                        (this means 3, 4, 5, 6, 7), or job files or
                        directories

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v         Increase verbosity
  --status {done,pend,exit,run}, -s {done,pend,exit,run}
                        Specify the status with which jobs qualify to be
                        resubmitted
  --force, -f           Kill the the bjobs cache file before doing anything
```

### view
```
usage: kong view [-h] [--verbose] [--force] [--status {done,pend,exit,run}]
                 tgt [tgt ...]

positional arguments:
  tgt                   Specify a job range. Can be '1 2 3', '1-3', '3+4'
                        (this means 3, 4, 5, 6, 7), or job files or
                        directories

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v         Increase verbosity
  --force, -f           Kill the bjobs cache
  --status {done,pend,exit,run}, -s {done,pend,exit,run}
                        Only show subjobs with this status
```
