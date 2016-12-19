#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function
import os
import sys
import argparse
from ConfigParser import SafeConfigParser
import fcntl
from contextlib import contextmanager
import datetime
import shutil
import logging
import collections
import multiprocessing as mp
import threading as th
import argcomplete
from glob import glob

import imp
from termcolor import colored

try:
    from humanize import naturaltime
    humanize_available = True
except ImportError:
    humanize_available = False


from lsf import *
from python_utils.printing import *
from log import logger
from JobDB import JobDB

""":type : JobDB"""
jobdb = None

sys.path.append('/project/atlas/software/python_include/')
from SUSYHelpers import *

__all__ = ["submit", "list_submit"]


JOBOUTDIR_FORMAT = "{jobid:05d}_{jobname}"
JOBSTDOUT_FORMAT = "{jobid:05d}_{subjobid:05d}.stdout"
JOBSTDERR_FORMAT = "{jobid:05d}_{subjobid:05d}.stderr"

CONFIG_TEMPLATE = """
[kong]
kongdir={kongdir}
registry={regdir}
output={outdir}
batch_cache_timeout = 60

[analysis]
# output=/etapfs02/atlashpc/pgessing/output/
# framework_job_script=/gpfs/fs1/home/pgessing/workspace_xAOD/AnalysisJob/runGenericJobMogon.py
# buildfile=/home/pgessing/workspace_xAOD/Configs/buildno
# tarball_dir=/home/pgessing/workspace_xAOD/input_tarballs
# binary=./Analysis
# algo=AlgoWPR
# base_release=Base,2.4.18
# application_profile=Reserve10G
# resource=rusage[atlasio=10]
# default_queue=atlasshort
"""
# input_tarball=/home/pgessing/workspace_xAOD/input_tarballs//input.tar


BASEDIR = os.path.dirname(os.path.realpath(__file__))

def mkdir(d):
    os.system("mkdir -p {}".format(d))

@contextmanager
def openlock(p, m):
    with open(p, m) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield f
        fcntl.flock(f, fcntl.LOCK_UN)
    

def get_config():
    home = os.path.expanduser("~")
    config_file = os.path.join(home, ".kongrc")

    first_time = False
    if not os.path.exists(config_file):
        first_time = True
        logger.info(".kongrc file not found, creating...")
        print("Where do you want the kong base directory to be? [~/kongdir]")
        kongdir = raw_input()
        if len(kongdir) == 0: kongdir = os.path.expanduser("~/kongdir")

        print("Where do you want the job registry to be? [{}/registry]".format(kongdir))
        regdir = raw_input()
        if len(regdir) == 0: regdir = os.path.expanduser("%(kongdir)s/registry")

        print("Where do you want the job std output to be? [{}/output]".format(kongdir))
        stdoutdir = raw_input()
        if len(stdoutdir) == 0: stdoutdir = os.path.expanduser("%(kongdir)s/output")

        with open(config_file, "w+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(CONFIG_TEMPLATE.format(kongdir=kongdir, regdir=regdir, outdir=stdoutdir))
            fcntl.flock(f, fcntl.LOCK_UN)
        

    
    cp = SafeConfigParser()
    cp.read(config_file)

    regdir = cp.get("kong", "registry")
    outdir = cp.get("kong", "output")

    # check folders
    if not os.path.exists(regdir):
        mkdir(regdir)

    if not os.path.exists(outdir):
        mkdir(outdir)

    if first_time:
        logger.info("Setup completed, you should probably have a look at {} before continuing".format(config_file))    
        sys.exit(0)


    return cp

def truncate_middle(str, length):
    if len(str) <= length:
        return str.ljust(length)
    
    part1 = str[:length/2-3]
    part2 = str[len(str)-(length/2-2):]

    outstr = (part1 + "(...)" + part2).ljust(length)
    return outstr

def get_directories(config):
    kongdir = os.path.realpath(config.get("kong", "kongdir"))
    regdir = os.path.realpath(config.get("kong", "registry"))
    outdir = os.path.realpath(config.get("kong", "output"))
    return (kongdir, regdir, outdir)

def get_tty_width():
    if not sys.stdin.isatty():
        return 90
    rows, columns = subprocess.check_output(['stty', 'size']).split()
    return int(columns)

def get_all_jobs(dir):
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(".job"):
                yield (root, file)

def find_job_files(dir):
    job_dict = {}
    for root, file in get_all_jobs(dir):
        jobid, _ = file.split("_", 1)
        job_dict[int(jobid)] = os.path.join(root, file)
    return job_dict

def parse_job_range_list(tgt):
    
    job_range = []

    # print(os.path.exists(tgt[0]))

    if all(os.path.exists(t) for t in tgt):
        logger.debug("rm: arguments are file or dir, file deletion mode")
        
        for jfile in tgt:
            if os.path.isdir(jfile):
                sub = parse_job_range_list([os.path.join(jfile, f) for f in os.listdir(jfile)])
                # print("subdir")
                # print(sub)
                for j in sub:
                    job_range.append(j)

            if not jfile.endswith(".job"):
                continue

            jobid, jobname = os.path.basename(jfile).split("_", 1)
            jobid = int(jobid)
            jobname = jobname[:-4]

            job_range.append(jobid)

            # logger.info("removing job {} : {}".format(jobid, jobname))

    else:
        if len(tgt) > 1 and all(t.isdigit() for t in tgt):
            job_range = [int(t) for t in tgt]
        elif len(tgt) == 1 and tgt[0].isdigit():
            job_range = [int(tgt[0])]
        elif "-" in tgt[0]:
            start, end = tgt[0].split("-")
            job_range = range(int(start), int(end)+1)
        elif "+" in tgt[0]:
            start, d = tgt[0].split("+")
            job_range = range(int(start), int(start) + int(d)+1)
        else:
            logger.error("Invalid input parameters")
            raise ValueError()
     
    return job_range

def remove_joboutput(jobid, outdir, regdir, analysis_output):
    logger.debug("removing job output for {}".format(jobid))
   
    logger.debug("remove stdout and stderr")
    
    jobid_padded = "{:05d}".format(jobid)
    outs = []
    for f in os.listdir(outdir):
        if f.startswith(jobid_padded):
            fullf = os.path.join(outdir, f)
            logger.debug("rm {}".format(fullf))
            try:
                os.remove(f)
            except: pass 

    logger.debug("remove analysis output")
    for f in os.listdir(analysis_output):
        if f.startswith(jobid_padded):
            fullf = os.path.join(analysis_output, f)
            logger.debug("rm {}".format(fullf))
            try:
                shutil.rmtree(fullf)
            except: pass

def sum_up_directory(dir):
    contents = os.listdir(dir)
    jobs = []

    minj = 100000
    maxj = 0

    for f in contents:
        fullf = os.path.join(dir, f)


        if os.path.isdir(fullf):
            sub = sum_up_directory(fullf)
            jobs += sub
            continue
        
        if not os.path.isfile(fullf) or not f.endswith(".job"):
            continue
        
        jobid, _ = f.split("_", 1)
        jobs.append(int(jobid))

    # print(lsfids)
    return jobs

def count_statuses(jobs):
    npend = 0
    ndone = 0
    nrun = 0
    nexit = 0
    nother = 0
    ntotal = len(jobs)

    # print(jobs)
    for j in jobs:
        # print(j)
        if j["stat"] == "DONE": ndone +=1
        elif j["stat"] == "PEND": npend +=1
        elif j["stat"] == "EXIT": nexit +=1
        elif j["stat"] == "RUN": nrun +=1
        else: nother += 1

    ngone = ntotal - (ndone + npend + nexit + nrun + nother)
   
    return npend, ndone, nrun, nexit, nother, ngone, ntotal

def make_status_string(ntotal, npend, ndone, nrun, nexit, nother):
    # npend = 0
    # ndone = 0
    # nrun = 0
    # nexit = 0
    # nother = 0
    # ntotal = 0
    #
    # # print(jobs)
    # for j in jobs:
    #     # print(j)
    #     if j["stat"] == "DONE": ndone +=1
    #     elif j["stat"] == "PEND": npend +=1
    #     elif j["stat"] == "EXIT": nexit +=1
    #     elif j["stat"] == "RUN": nrun +=1
    #     else: nother += 1
    #
    #     ntotal += 1

    status_string = "{p:>4d} P | {r:>4d} R | {d:>4d} D | {e:>4d} E | {o:>4d} O".format(p=npend, d=ndone, r=nrun, e=nexit, o=nother)
    
    color = "white"
    if nexit > 0:
        color = "red"
    elif npend > 0:
        color = "yellow"
    elif nrun > 0:
        color = "blue"
    elif ndone == ntotal or ntotal == 0:
        color = "green"

    return (status_string, color, (
        npend,
        ndone,
        nrun,
        nexit,
        nother
    ))

def get_job_id(dir, dry=False):

    idfile = os.path.join(dir, "current_jobid")

    if not os.path.exists(idfile):
        if dry:
            return 1
        else:
            with open(idfile, "w+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                f.write("0")
                fcntl.flock(f, fcntl.LOCK_UN)

    with open(idfile, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        i = int(f.read())+1
        if not dry:
            f.seek(0)
            f.write(str(i))
        fcntl.flock(f, fcntl.LOCK_UN)
    return i

def submit(cmds, name, config=None, queue=None, R=None, app="Reserve2G", W=300, dir=None, verbosity=None, dry_run=False):
    "Submit an array of lsf jobs"
    if verbosity > 1:
        logger.setLevel(logging.DEBUG)
    elif verbosity > 0:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

    # if verbosity != None:
        # if verbosity > 1:
            # logger.setLevel(logging.DEBUG)
        # elif verbosity > 0:
            # logger.setLevel(logging.INFO)
        # else:
            # logger.setLevel(logging.WARNING)

    if len(cmds) == 0:
        logger.error("Lists input was empty")
        raise ValueError()

    if not config:
        config = get_config()

    if queue == None:
        queue = config.get("analysis", "default_queue")

    width = get_tty_width()

    logger.info("Submitting {} LSF jobs".format(len(cmds)))
   

    print()
    print("job name:", name)
    if verbosity > 0:
        print(width*"-")
        for cmd in cmds:
            print(cmd)
        print(width*"-")


    kongdir, regdir, outdir = get_directories(config)
    global jobdb
    if jobdb == None:
        jobdb = get_default_jobdb(kongdir, config.getint("kong", "batch_cache_timeout"))

    submit_dir = dir if dir else regdir
    if not os.path.isabs(submit_dir):
        submit_dir = os.path.join(regdir, dir)
    
    logger.info("Submitting to {}".format(submit_dir))
    logger.info("Submitting to queue {}".format(queue))
    logger.info("Walltime is {}".format(W))
    
    blacklist_string = str(GetFullBlacklist(3)).strip()
    if len(blacklist_string) > 0: " && "+blacklist_string

    submit_tasks = []
        
    jobid = get_job_id(kongdir, dry=dry_run)
    subjobid = 0
        
    if not dry_run:
        jobfile = open(os.path.join(submit_dir, make_jobfile_name(jobid, name)), "w")

    for subjobid, cmd in enumerate(cmds):
        stdoutfile = os.path.join(outdir, JOBSTDOUT_FORMAT.format(jobid=jobid, subjobid=subjobid))
        stderrfile = os.path.join(outdir, JOBSTDERR_FORMAT.format(jobid=jobid, subjobid=subjobid))
           
        submit_tasks.append(submit_task(
            jobid=jobid,
            subjobid=subjobid,
            jobname=name,
            # cmd=cmd,
            stdout=stdoutfile,
            stderr=stderrfile,
            exe=cmd,
            W=W, 
            app=app, 
            R=R, 
            queue=queue,
            dry=dry_run
        ))
    
    spinner = Spinner("Submitting tasks to the batch system")
    def tick(n):
        perc = n/float(len(submit_tasks))*100
        spinner.next("{}/{} {:.2f}%".format(n, len(submit_tasks), perc))

    submit_results = thread_map(submit_thread, submit_tasks, tick=tick)
    spinner.finish()
   
    

    for jobid, subjobid, subjoblsfid in submit_results:
        if not dry_run:
            logger.debug("{}:{}\n".format(subjobid, subjoblsfid))
            jobfile.write("{}:{}\n".format(subjobid, subjoblsfid))
            jobdb.register_subjob(jobid, subjobid, subjoblsfid)

    if not dry_run:
        logger.debug("closing jobfile handle")
        jobfile.close()

    logger.debug("invalidating dbfile file {}".format(jobdb.dbfile))
    jobdb.invalidate()


def make_jobfile_name(jobid, jobName):
    return "{:05d}_{}.job".format(jobid, jobName)

def cli_submit(args, config):
    """
    Submit job cmd file, one lsf job per line
    """

    cmds = []
    if not args.submit_file:
        logger.debug("No submit file given, try reading from stdin")
        cmds = sys.stdin.read().split("\n")[:-1]
        
        if not args.name:
            raise ValueError("Cannot omit --name when using stdin")
        
        name = args.name
    else:
        logger.debug("Reading input job submit file")
        cmds = args.submit_file.read().split("\n")[:-1]

    
    submit(
        cmds=cmds,
        name=name,
        config=config,
        app=args.app,
        R=args.R,
        W=args.W,
        queue=args.queue,
        dir=os.getcwd(),
        dry_run=args.dry_run
    )


def list_submit(lists, config=None, queue=None, dir=None, sys=False, verbosity=None, buildno=None, dry_run=False):
    """
    Input format for lists is a list of tuples with (NAME, LISTFILE).
    """

    from FileSplitterUtils import splitFileListBySizeOfSubjob
    if verbosity > 1:
        logger.setLevel(logging.DEBUG)
    elif verbosity > 0:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

    # if verbosity != None:
        # if verbosity > 1:
            # logger.setLevel(logging.DEBUG)
        # else:
            # logger.setLevel(logging.INFO)

    if len(lists) == 0:
        logger.error("Lists input was empty")
        raise ValueError()

    if not config:
        config = get_config()

    if queue == None:
        queue = config.get("analysis", "default_queue")

    width = get_tty_width()

    logger.info("Submitting {} lists".format(len(lists)))
   
    # print([(len(n), n) for n, _ in lists])
    nlen = max([len(n) for n, _ in lists])
    # print(nlen)
    nlen = max(nlen, len("job name"))
    nlen = min(nlen, width/2)

    print_lists = [l for _, l in lists]
    common_prefix = os.path.dirname(os.path.commonprefix(print_lists))

    print()
    print("job name".ljust(nlen), "|", "list file".ljust(width-nlen-3))
    print("-"*(nlen+1)+"|"+"-"*(width-nlen-2))

    for name, list in lists:
        list = os.path.relpath(list, common_prefix)
        print(truncate_middle(name, nlen), "|", truncate_middle(list, width-nlen-3))

    
    kongdir, regdir, outdir = get_directories(config)
    global jobdb
    if jobdb == None:
        jobdb = get_default_jobdb(kongdir, config.getint("kong", "batch_cache_timeout"))

    submit_dir = dir if dir else regdir
    if not os.path.isabs(submit_dir):
        submit_dir = os.path.join(regdir, dir)
    
    logger.info("Submitting to {}".format(submit_dir))
    
    name_list_splits = [(n, l, splitFileListBySizeOfSubjob(l, 5.5)) for n, l in lists]

    analysis_outdir = config.get("analysis", "output")
    # input_tarball = config.get("analysis", "input_tarball")
    binary = config.get("analysis", "binary")
    algo = config.get("analysis", "algo")
    base_release = config.get("analysis", "base_release")

    if buildno == None:
        buildfile = config.get("analysis", "buildfile")
        if not os.path.exists(buildfile):
            raise runtime_error("Buildfile {} does not exist".format(buildfile))
        with open(buildfile, "r") as f:
            buildno = int(f.read())

    # find input tarball for build no
    candidates = glob(os.path.join(config.get("analysis", "tarball_dir"), "*build{:03d}*.tar".format(buildno)))
    if len(candidates) == 0:
        errorblock("Build tarball for {} not found".format(buildno))
        sys.exit(1)
    elif len(candidates) > 1:
        errorblock("Build tarball for {} ambiguous".format(buildno))
        sys.exit(1)
    
    input_tarball = candidates[0]

    print()
    print(block([
        "Framework job with following options:",
        "Output:        "+analysis_outdir,
        "Base release:  "+base_release,
        "Binary:        "+binary,
        "Algo:          "+algo,
        "Systematics:   "+("yes" if sys else "no"),
        "Build:         "+str(buildno),
        "Input tarball: "+input_tarball
    ], char="-"))
    print()

    blacklist_string = str(GetFullBlacklist(3)).strip()
    if len(blacklist_string) > 0: " && "+blacklist_string

    submit_tasks = []
    jobfiles = {}

    for jobName, list, splits in name_list_splits:
        jobid = get_job_id(kongdir, dry=dry_run)
        subjobid = 0
       
        joboutdir = os.path.join(analysis_outdir, JOBOUTDIR_FORMAT.format(jobid=jobid, jobname=jobName))

        if not dry_run:
            mkdir(joboutdir)
            jobfile = open(os.path.join(submit_dir, make_jobfile_name(jobid, jobName)), "w")
            jobfiles[jobid] = jobfile

        

        for split in splits:
            stdoutfile = os.path.join(outdir, JOBSTDOUT_FORMAT.format(jobid=jobid, subjobid=subjobid))
            stderrfile = os.path.join(outdir, JOBSTDERR_FORMAT.format(jobid=jobid, subjobid=subjobid))
           
            composedOptions = '\'{algo} -p 0. -n 0{sys} --wn --wnDir . \''.format(
                    algo=algo,
                    sys=" --sys" if sys else ""
                )
            
            logger.debug(composedOptions)

            cmd = [
                config.get("analysis", "framework_job_script"), 
                '--binary', binary, 
                '--composedOptions', composedOptions,
                '--filelist', list, 
                '--outputMask', 'Plots\\*.root', 
                '--outputDir', joboutdir,
                '--listRanges', split,
                '--inputTarballName', input_tarball, 
                '--jobid', str(jobid), 
                '--subjobid', str(subjobid), 
                '--jobname', jobName,
                '--analysisRelease', base_release, 
                '--isFrameworkJob', '1', 
                '--jobSplittingMode', 'Size', 
                '--usePackedTarball'
            ]
            
            submit_tasks.append(submit_task(
                jobid=jobid,
                subjobid=subjobid,
                jobname=jobName,
                # cmd=cmd,
                stdout=stdoutfile,
                stderr=stderrfile,
                exe=" ".join(cmd), 
                W=300, 
                app=config.get("analysis", "application_profile"), 
                R=config.get("analysis", "resource")+blacklist_string, 
                queue=queue,
                dry=dry_run
            ))
            subjobid +=1
    
    spinner = Spinner("Submitting tasks to the batch system")
    def tick(n):
        perc = n/float(len(submit_tasks))*100
        spinner.next("{}/{} {:.2f}%".format(n, len(submit_tasks), perc))

    submit_results = thread_map(submit_thread, submit_tasks, tick=tick)
    spinner.finish()

    for jobid, subjobid, subjoblsfid in submit_results:
        # print(subjobid, subjoblsfid)
        if not dry_run:
            jobfiles[jobid].write("{}:{}\n".format(subjobid, subjoblsfid))
            jobdb.register_subjob(jobid, subjobid, subjoblsfid)
        jobdb.commit()

    if not dry_run:
        logger.debug("closing jobfile handles")
        for key, f in jobfiles.iteritems():
            f.close()

    logger.debug("invalidating dbfile file {}".format(jobdb.dbfile))
    jobdb.invalidate()


def submit_thread(t):
   
    subjoblsfid = bsub(
        exe=t.exe,
        W=t.W,
        app=t.app,
        R=t.R,
        queue=t.queue,
        stdout=t.stdout,
        stderr=t.stderr,
        name="{:05d}_{:05d}_{}".format(t.jobid, t.subjobid, t.jobname),
        dry=t.dry
    )
    
    logger.debug("{}.{} => {} submitted to LSF".format(t.jobid, t.subjobid, subjoblsfid))

    # if not dry_run:
        # jobfile.write("{}:{}\n".format(t.subjobid, subjoblsfid))

    return (t.jobid, t.subjobid, subjoblsfid)

def thread_map(func, values, threadcount=mp.cpu_count(), tick=lambda x:x, tick_throttle=0.1):
    q = collections.deque(values)
    res_q = collections.deque()

    def worker():
        while True:
            try:
                result = func(q.pop())
                res_q.appendleft(result)
            except IndexError:
                break

    threads = []

    for i in range(threadcount):
        t = th.Thread(target=worker)
        t.daemon = True
        t.start()
        threads.append(t)

    while len(q) > 0:
        time.sleep(tick_throttle)
        tick(len(res_q))

    for t in threads:
        t.join()
        tick(len(res_q))

    return list(res_q)


def get_subjob_ids(fullf):
    f = os.path.basename(fullf)

    jobid, name = f[:-4].split("_", 1)
    with open(fullf, "r") as jobf:
        subjobs = jobf.read().split("\n")[:-1]

    if len(subjobs) == 0:
        return []

    subjobids = []
    for sji in subjobs:
        i, lsfid = sji.split(":")
        subjobids.append(lsfid)
    
    return subjobids


#################
# CLI FUNCTIONS #
#################

def get_default_jobdb(kongdir, timeout):
    return JobDB(dbfile=os.path.join(kongdir, "kong.sqlite"), timeout=timeout)

def main():
    """
    kong submits jobs to LSF and keeps track of them, with a directory hierarchy of .job files
    """
    
    # this is to catch first time users only calling the command itself
    logger.setLevel(logging.INFO)
    config = get_config()
    
    kongdir, regdir, outdir = get_directories(config)

    # set up the global jobdb

    global jobdb
    jobdb = get_default_jobdb(kongdir, config.getint("kong", "batch_cache_timeout"))

    # setting environment variables

    # home = os.path.expanduser("~")
    # config_file = os.path.join(home, ".kongrc")
    # if not os.path.exists(config_file):
        # get_config()

    
    JOB_RANGE_HELP = "Specify a job range. Can be '1 2 3', '1-3', '3+4' (this means 3, 4, 5, 6, 7), or job files or directories"

    parser = argparse.ArgumentParser(description=main.__doc__)
    subparsers = parser.add_subparsers()
    
    parentp = argparse.ArgumentParser(add_help=False)
    parentp.add_argument("--verbose", "-v", action="count", help="Increase verbosity")

    peekp = subparsers.add_parser("peek", parents=[parentp], help=peek.__doc__)
    peekp.set_defaults(func=peek)
    peekp.add_argument("stream", choices=["out", "err"], help="Which stream to peek into")
    peekp.add_argument("jobid", type=int, help="The job id to peek into")
    peekp.add_argument("subjobid", type=int, help="The subjobid to peek into")

    lsubmitp = subparsers.add_parser("list-submit", aliases=["lsubmit"], parents=[parentp], help=cli_list_submit.__doc__)
    lsubmitp.set_defaults(func=cli_list_submit)
    lsubmitp.add_argument("lists", nargs="+", help="The lists to submit. You can glob in the shell")
    lsubmitp.add_argument("--name", help="Name to assign to the job. Only works when a single list is given")
    lsubmitp.add_argument("--dry-run", action="store_true", help="Don't really do anything")
    lsubmitp.add_argument("--queue", "-q", help="Submit to this queue")
    lsubmitp.add_argument("--build", "-b", type=int, help="Which build number tarball should be used. Defaults to the latest one")
    lsubmitp.add_argument("--sys", action="store_true", help="Include systematics")

    submitp = subparsers.add_parser("submit", parents=[parentp], help=cli_submit.__doc__)
    submitp.set_defaults(func=cli_submit)
    submitp.add_argument("submit_file", nargs="?", type=file, help="a file with one job command per line")
    submitp.add_argument("--name", help="Name to assign to the job. If not specified, submit file name is used")
    submitp.add_argument("--dry-run", action="store_true", help="Don't really do anything")
    submitp.add_argument("--queue", "-q", help="Submit to this queue")
    submitp.add_argument("-R")
    submitp.add_argument("--app")
    submitp.add_argument("-n")
    submitp.add_argument("-W")
    
    # jobdirp = subparsers.add_parser("jobdir", aliases=['jd'] parents=[parentp], help=cli_jobdir.__doc__)
    # jobdirp.set_defaults(func=cli_jobdir)
    
    cdp = subparsers.add_parser("cd", parents=[parentp], help=cd.__doc__)
    cdp.set_defaults(func=cd)


    lsp = subparsers.add_parser("ls", parents=[parentp], help=ls.__doc__)
    lsp.set_defaults(func=ls)
    lsp.add_argument("dir", nargs="?", default=os.getcwd(), help="The directory or files to list. You can glob")
    lsp.add_argument("--all", "-a", action="store_true", help="If given, list all jobs found in the registry")
    lsp.add_argument("--force", "-f", action="store_true", help="Kill the bjobs cache")
    if humanize_available:
        lsp.add_argument("--human", "-H", action="store_true", help="Human readable output of times")

    monitorp = subparsers.add_parser("monitor", parents=[parentp], help=monitor.__doc__)
    monitorp.set_defaults(func=monitor)
    monitorp.add_argument("dir", nargs="?", default=os.getcwd(), help="The directory or files to list. You can glob")
    monitorp.add_argument("--all", "-a", action="store_true", help="If given, list all jobs found in the registry")
    excl = monitorp.add_mutually_exclusive_group()
    excl.add_argument("--serve", action="store_true")
    excl.add_argument("--push", action="store_true")

    pushp = subparsers.add_parser("push", parents=[parentp])
    pushp.set_defaults(func=push)

    renamep = subparsers.add_parser("rename", aliases=["rn"], parents=[parentp], help=rename.__doc__)
    renamep.set_defaults(func=rename)
    renamep.add_argument("orig", help="Original file, or job id")
    renamep.add_argument("dest", help="Destination job name. Job id will be preserved.")

    rmp = subparsers.add_parser("remove", aliases=["rm"], parents=[parentp], help=rm.__doc__)
    rmp.set_defaults(func=rm)
    rmp.add_argument("tgt", nargs="+", help=JOB_RANGE_HELP)

    killp = subparsers.add_parser("kill", parents=[parentp], help=kill.__doc__)
    killp.set_defaults(func=kill)
    killp.add_argument("tgt", nargs="+", help=JOB_RANGE_HELP)

    recoverp = subparsers.add_parser("recover", parents=[parentp], help=recover.__doc__)
    recoverp.set_defaults(func=recover)
    recoverp.add_argument("job", type=int, help="Job id, for which to attempt recovery")

    resubmitp = subparsers.add_parser("resubmit", parents=[parentp], help=resubmit.__doc__)
    resubmitp.set_defaults(func=resubmit)
    resubmitp.add_argument("tgt", nargs="+", help=JOB_RANGE_HELP)
    resubmitp.add_argument("--status", "-s", choices=["done", "pend", "exit", "run", "unkwn"], default="exit", help="Specify the status with which jobs qualify to be resubmitted")
    resubmitp.add_argument("--force", "-f", action="store_true", help="Kill the the bjobs cache file before doing anything")
    resubmitp.add_argument("--yes", "-y", action="store_true", help="Yep! If anyone asks")
    resubmitp.add_argument("--interval", "-i", type=int)

    viewp = subparsers.add_parser("view", parents=[parentp], help=view.__doc__)
    viewp.set_defaults(func=view)
    viewp.add_argument("tgt", nargs="+", help=JOB_RANGE_HELP)
    viewp.add_argument("--force", "-f", action="store_true", help="Kill the bjobs cache")
    viewp.add_argument("--status", "-s", choices=["done", "pend", "exit", "run", "unkwn"], help="Only show subjobs with this status")
    viewp.add_argument("--cmd", "-c", action="store_true", help="Show the command of the job")
    
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    
    if not humanize_available:
        args.human = False

    print()

    if args.verbose > 1:
        logger.setLevel(logging.DEBUG)
    elif args.verbose > 0:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)


    args.func(args, config)

def cd(args, config):
    kongdir, regdir, outdir = get_directories(config)
    print(regdir)

def recover(args, config):
    """
    Attempt to recover a lost jobs file from bjobs output
    """

    out = subprocess.check_output("bjobs -wa | grep {:05d}_".format(args.job), shell=True)
    lines = out.split("\n")[:-1]

    info = []
    jobname = ""

    for l in lines:
        spl = l.split(" ")
        lsf = spl[0]
        name = spl[-4]
        print(spl, name)
        jobid, subjobid, label = name.split("_", 2)
        info.append((int(subjobid),lsf))
        jobname = jobid+"_"+label+".job"

    info = sorted(info, key=lambda j:j[0])
    info = [str(j)+':'+l for j,l in info]

    with open(os.path.join(os.getcwd(), jobname), "w+") as f:
        f.write("\n".join(info))


    print("DONE")

    # print(info)
    # print(out

def resubmit(args, config):
    """
    Requeue subjobs with a specified status back to the batch system
    """

    tgt = args.tgt
    kongdir, regdir, outdir = get_directories(config)
    analysis_output = config.get("analysis", "output")
    
    # jobcachefile = os.path.join(kongdir, "bjobs_cache")
    # if args.force:
    jobdb.update(force=args.force)
        # os.remove(jobcachefile)
    
    job_range = parse_job_range_list(tgt)
    all_jobfiles = find_job_files(regdir)



    print("Looking for lsf jobs with status {} in job(s) {}".format(bold(args.status.upper()), ", ".join(str(j) for j in job_range)))

    def do_resubmit():
        selected = []
        # job_info = get_job_info(jobcachefile)
        
        for jobid in job_range:
            if not jobid in all_jobfiles:
                logger.warning("job with id {} is not found in registry".format(jobid))
                continue

            path = all_jobfiles[jobid]

            for subjob in jobdb.getBatchJobsForJob(jobid):
                    subjobid, subjoblsfid = subjob["subjobid"], subjob["batchjobid"]
                    stat = subjob["stat"]
                    if stat == args.status.upper():
                        selected.append(subjoblsfid)

        if not args.yes:
            if not confirm("Resubmitting the following {} LSF jobs: {}".format(len(selected), ", ".join(selected))):
                return
        
        for s in selected:
            if args.status == "unkwn":
                mode = ""
            else:
                mode = args.status[0].lower()
            print(s)
            try:
                brequeue(s, mode)
            except:
                print(colored("Error requeueing {}".format(s), "white", "on_red"))
        

    if args.interval:
        interval = max(30, args.interval)
        i = interval
    
        while True:
            if i >= interval:
                sys.stdout.write("\r")
                print("Updating job database")
                jobdb.update(force=True)
                print("Checking / Resubmitting")
                do_resubmit()
                i = 0

            n = i % 4
            spin = "."*(n) + "," + "."*(3-n)
            sys.stdout.write(("\rWaiting for {:"+str(len(str(interval)))+"d}s {: <5}").format(interval-i, spin))
            sys.stdout.flush()
            i += 1
            time.sleep(1)

    else:
        do_resubmit()

        jobdb.update(force=True)



def view(args, config):
    """
    Show the subjobs and their statuses for a job range
    """

    tgt = args.tgt
    kongdir, regdir, outdir = get_directories(config)
    analysis_output = config.get("analysis", "output")
    
    # jobcachefile = os.path.join(kongdir, "bjobs_cache")
    # if args.force and os.path.exists(jobcachefile):
    #     os.remove(jobcachefile)

    jobdb.update(force=args.force)

    job_range = parse_job_range_list(tgt)
    all_jobfiles = find_job_files(regdir)

    width = get_tty_width()

    for jobid in job_range:
        if not jobid in all_jobfiles:
            logger.warning("job with id {} is not found in registry".format(jobid))
            continue

        path = all_jobfiles[jobid]
        
        # print("JOB", jobid)
        # with open(path, "r") as f:
        #     for l in f.read().split("\n")[:-1]:
        #         subjobid, subjoblsfid = l.split(":")
        #         lsfjob = jobdb.get(subjoblsfid)
        #
        #         stat = lsfjob["stat"]
        #
        #         if args.status and stat != args.status.upper():
        #             continue
        #
        #         outstr = "{:>5} | {:>8} | {}".format(subjobid, subjoblsfid, stat.ljust(4))
        #
        #
        #         color = "white"
        #         if stat == "PEND":
        #             color = "yellow"
        #         elif stat == "RUN":
        #             color = "blue"
        #         elif stat == "DONE":
        #             color = "green"
        #         elif stat == "EXIT":
        #             color = "red"
        #
        #         print(colored(outstr, color))

        for subjob in jobdb.getBatchJobsForJob(jobid):
            stat = subjob["stat"]
            if args.status and stat != args.status.upper():
                continue

            color = "white"
            if stat == "PEND":
                color = "yellow"
            elif stat == "RUN":
                color = "blue"
            elif stat == "DONE":
                color = "green"
            elif stat == "EXIT":
                color = "red"

            outstr = colored("{:>5} : {:<5} | {:>8} | {} | {q} | {h}".format(
                jobid,
                subjob["subjobid"],
                subjob["batchjobid"],
                stat.ljust(4),
                q = subjob["queue"],
                h = subjob["exec_host"]
            ), color)

            if args.cmd:
                outstr += "\n"+subjob["cmd"]+"\n"

            print(outstr)







def kill(args, config):
    """
    Kill a job or range of jobs. Note that it can take a moment, before kong ls sees the status changes
    """

    tgt = args.tgt
    kongdir, regdir, outdir = get_directories(config)
    analysis_output = config.get("analysis", "output")
   
    job_range = parse_job_range_list(tgt)

    if not confirm("Killing jobs {}".format(", ".join(str(j) for j in job_range))):
        logger.info("Aborting")
        return

    all_jobfiles = find_job_files(regdir)

    kills = []

    for jobid in job_range:
        if not jobid in all_jobfiles:
            logger.warning("job with id {} is not found in registry".format(jobid))
            continue
        
        logger.info("killing job {}".format(jobid))
        path = all_jobfiles[jobid]
        
        with open(path, "r") as f:
            # subjobs = [ l.split(":") for l in f.read().split("\n")[:-1]]
            for l in f.read().split("\n")[:-1]:
                subjobid, subjoblsfid = l.split(":")
                kills.append((jobid, subjobid, subjoblsfid))

    spinner = Spinner("Killing batch system tasks")
    def tick(n):
        perc = n/float(len(kills))*100
        spinner.next("{}/{} {:.2f}%".format(n, len(kills), perc))

    def dokill(k):
        jobid, subjobid, subjoblsfid = k
        bkill(subjoblsfid)
        logger.info("Job {}.{} => {} killed".format(jobid, subjobid, subjoblsfid))


    results = thread_map(dokill, kills, tick=tick)
    spinner.finish()
    
    jobcachefile = os.path.join(kongdir, "bjobs_cache")
    logger.debug("killing bjobs cache file {}".format(jobcachefile))

    jobdb.invalidate()

        # for subjobid, subjoblsfid in subjobs:
            # bkill(subjoblsfid)
            # logger.info("Job {}.{} => {} killed".format(jobid, subjobid, subjoblsfid))

def rm(args, config):
    """
    Remove a job or range of jobs. This removes the job file, job stdout and stderr files as well as the
    analysis output. You might want to kill the job(s) first.
    """

    tgt = args.tgt
    kongdir, regdir, outdir = get_directories(config)
    analysis_output = config.get("analysis", "output")
    
    job_range = parse_job_range_list(tgt)


    if not confirm("Removing jobs {}".format(", ".join(str(j) for j in job_range))):
        logger.info("Aborting")
        return
    

    # find where job file is
    all_jobfiles = find_job_files(regdir)
    for jobid in job_range:
        if not jobid in all_jobfiles:
            logger.warning("job with id {} is not found in registry".format(jobid))
            continue
        logger.info("removing job {}".format(jobid))
        path = all_jobfiles[jobid]
        logger.debug("remove {}".format(path))
        
        try:
            os.remove(path)
        except: pass

        remove_joboutput(jobid, outdir, regdir, analysis_output)

def monitor(args, config):
    import curses
    import BaseHTTPServer
    import requests
    import json
    
    args.force = False
    args.human = False

    interval = 30
    
    
    stdscr = curses.initscr()
    curses.noecho()
    curses.start_color()
    curses.use_default_colors()
    curses.cbreak()

    curses.init_pair(1, curses.COLOR_WHITE, -1)
    curses.init_pair(2, curses.COLOR_RED, -1)
    curses.init_pair(3, curses.COLOR_GREEN, -1)
    curses.init_pair(4, curses.COLOR_BLUE, -1)
    curses.init_pair(5, curses.COLOR_YELLOW, -1)

    color_map = {
        # "white": 7,
        # "red": 1,
        # "green": 2,
        # "blue": 4,
        # "yellow": 3
        "white": 1,
        "red": 2,
        "green": 3,
        "blue": 4,
        "yellow": 5
    }

    lines = None
    
    HTML_TEMPLATE = ""
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "standalone.html"), "r") as f:
        HTML_TEMPLATE = f.read()

    class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(s):
            s.send_response(200)
            s.end_headers()

            if s.path.endswith("/data"):
                
                out = [";".join(map(str, l)) for l in lines]
                # print(out)

                s.wfile.write("\n".join(out))
            else:
                s.wfile.write(HTML_TEMPLATE)
        def log_request(s, *a):
            pass

        def log_error(s, *a):
            pass
    

    httpd = BaseHTTPServer.HTTPServer(("localhost", 8790), HttpHandler)
    
    # shutdown = Fals
    def server():
        httpd.serve_forever()

    t = th.Thread(target=server)

    if args.serve:
        t.start()   

    try:
        round = 1
        refresh_time = datetime.datetime.now()
        while True:
            # print("go")
            now = datetime.datetime.now()
            stdscr.addstr(0, 0, "kong")
            stdscr.addstr(1, 0, "Monitoring: {}".format(args.dir))
            stdscr.addstr(2, 0, "Time: {} - Last refresh: {}".format(now.strftime("%H:%M:%S %d.%m.%Y"), refresh_time.strftime("%H:%M:%S %d.%m.%Y")))
            width = get_tty_width()
            stdscr.addstr(3, 0, "-"*width)
           
            next = "Next refresh in {:>2d}s".format(interval - round)
            stdscr.addstr(0, width-len(next), next)
            
            if round >= interval or lines == None:
                round = 0
                lines = ls(args, config, noprint=True)
                refresh_time = datetime.datetime.now()
                if args.push:
                    requests.post(
                            "http://kong.paulgessinger.com/push?secret=1e277a82-d1dd-491b-98da-4f5c6ee109ac", 
                            data={"data":json.dumps(lines)}
                    ) 


            for i, l in enumerate(lines):
                string, color, info = l
                stdscr.addstr(4+i, 0, string, curses.color_pair(color_map[color]))

            stdscr.addstr(4+len(lines), 0, "-"*width)
            stdscr.refresh()
            time.sleep(1)
            round += 1
    except KeyboardInterrupt:
        pass
    finally:
        print("waiting for thread to finish")
        curses.echo()
        curses.nocbreak()
        curses.endwin()

        if args.serve:
            httpd.shutdown()
            t.join()
        # print("end")

def rename(args, config):
    """
    Rename a job without affecting the jobid
    """
    kongdir, regdir, outdir = get_directories(config)

    if os.path.exists(args.orig) and args.orig.endswith(".job"):
        jobfile = args.orig
        jobid, _ = os.path.basename(jobfile).split("_", 1)
    elif args.orig.isdigit():
        jobid = int(args.orig)
        jobfiles = get_all_jobs(regdir)
        for d, f in jobfiles:
            if f.startswith("{:05d}".format(jobid)):
                jobfile = os.path.join(d, f)
                break

    _, jobname = jobfile[:-4].split("_", 1)
    # print(jobname, jobid, jobfile)
    print("Renaming Job {} to {}".format(jobname, args.dest))

    cmd = "mv {} {:05d}_{}.job".format(jobfile, jobid, args.dest)
    os.system(cmd)



def push(args, config):
    import requests
    import json
    
    kongdir, regdir, outdir = get_directories(config)

    while True:
        jobfiles = get_all_jobs(regdir)
        jobfiles = [os.path.join(d, f) for d, f in jobfiles]

        p = r = d = e = o = 0

        lsfids = []

        for fullf in jobfiles:
            with open(fullf, "r") as f:
                lines = f.read().split("\n")[:-1]
                for l in lines:
                    _, i = l.split(":")
                    lsfids.append(i)

        jobcachefile = os.path.join(kongdir, "bjobs_cache")
        subjobinfo = get_job_info(jobcachefile, lsfids)
            
        for inf in subjobinfo:
            s = inf["stat"]
            
            if s == "DONE":
                d += 1
            elif s == "PEND":
                p += 1
            elif s == "RUN":
                r += 1
            elif s == "EXIT":
                e += 1
            else:
                o += 1
        
        monitor_push_url = config.get("kong", "monitor_push_url")
        # print(p, d, r, e, o)

        requests.post(
            monitor_push_url,
            data={"data":json.dumps({"p": p, "r": r, "d": d, "e": e, "o": o})}
        ) 
        time.sleep(30)



        # f = os.path.basename(fullf)

def sum_columns(x, y):
    res = []
    print(x, y)
    for i in range(0, len(x)):
        res = x[i] + y[i]

    return res

def ls(args, config, noprint=False):
    """
    Show information over job files or directory hierarchies with job files
    """

    kongdir, regdir, outdir = get_directories(config)
    # jobcachefile = os.path.join(kongdir, "bjobs_cache")
    #
    if args.dir == "*":
        return

    # check if dir is in another one
    if not os.path.realpath(args.dir).startswith(regdir):
        raise ValueError("Dir {} is not inside registry dir {}".format(args.dir, regdir))

    jobdb.update(force=args.force)

    dirs = []
    dir = "."

    if args.all:
        jobfiles = get_all_jobs(regdir)
        jobfiles = sorted(jobfiles, key=lambda j:j[1])
        jobfiles = [os.path.join(d, f) for d, f in jobfiles]
    else:
        dir = args.dir
        contents = os.listdir(dir)
        dirs = filter(lambda f: os.path.isdir(os.path.join(dir, f)), contents)
        dirs = [os.path.join(dir, f) for f in dirs]
        jobfiles = filter(lambda f: os.path.isfile(os.path.join(dir, f)) and f.endswith(".job"), contents)
        jobfiles = sorted(jobfiles)
        jobfiles = [os.path.join(dir, f) for f in jobfiles]


    if len(jobfiles) == 0 and len(dirs) == 0:
        print(block("no jobfiles found in this directory", char="-"))
        os.system("pwd && ls -l")
        return
        
    outlines = []

    width = get_tty_width()

    for subdir in dirs:
        jobs_in_dir = sum_up_directory(subdir)
        if len(jobs_in_dir) == 0:
            continue

        jobinfo = [sum(i) for i in zip(*map(jobdb.getJobInfo, jobs_in_dir))]

        # print(jobinfo)
        status_string, color, info = make_status_string(*jobinfo)
        outstr = " "*6 + "| {name} ({min}-{max}) | {status}".format(
            name="{name}",
            status=status_string,
            min=min(jobs_in_dir),
            max=max(jobs_in_dir)
        )

        name = os.path.relpath(subdir, dir)
        outstr = outstr.format(name=truncate_middle(name, width - len(outstr) + len("{name}")))
        # print(colored(outstr, color))
        if not noprint:
            print(colored(outstr, color))
        else:
            outlines.append((outstr, color, info))




    for fullf in jobfiles:
        f = os.path.basename(fullf)

        jobid, name = f[:-4].split("_", 1)
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fullf))

        # with open(fullf, "r") as jobf:
        #     subjobs = jobf.read().split("\n")[:-1]

        # if len(subjobs) == 0:
            # print(" "*6+"| "+fullf)
            # continue

        # subjobids = []
        # for sji in subjobs:
        #     i, lsfid = sji.split(":")
        #     subjobids.append(lsfid)
        
        # if args.all:
        #     bd = os.path.relpath(os.path.dirname(fullf), regdir)
        #     name = bd + "/" + name

        jobinfo = jobdb.getJobInfo(int(jobid))
        status_string, color, info = make_status_string(*jobinfo)

        if args.human:
            date = naturaltime(datetime.datetime.now() - mtime).ljust(14)
        else:
            date = mtime.strftime("%H:%M:%S %d.%m.%Y")

        outstr = "{jobid: 5d} | {name} | {date} | {status}".format(
            jobid=int(jobid), 
            name="{name}",
            date=date, 
            status=status_string
        )
       
        outstr = outstr.format(name=truncate_middle(name, width - len(outstr) + len("{name}")))
        

        # print(outstr)
        # print(ndone, npend, nexit, nrun, nother, ngone)
        if not noprint:
            print(colored(outstr, color))
        else:
            outlines.append((outstr, color, info))
    
    if noprint:
        return outlines

def peek(args, config):
    """
    Show the stdout or stderr of a given subjob
    """

    kongdir, regdir, outdir = get_directories(config)

    f = os.path.join(outdir, "{:05d}_{:05d}.std{}".format(args.jobid, args.subjobid, args.stream))

    os.system("less {}".format(f))



def cli_list_submit(args, config):
    """
    Submits jobs to the batch system. Input is one or multiple lists. Job files are created in the
    current working directory.
    """

    lists = []
    
    if len(args.lists) == 1 and args.name:
        name = args.name
        lists = [(name, args.lists[0])]
    elif len(args.lists) > 0:
        lists = []
        for list in args.lists:
            if not os.path.exists(list):
                continue
            bn = os.path.basename(list)
            
            if bn.endswith(".list"):
                bn = bn[0:-5]
            lists.append((bn, list)) 

    list_submit(
        lists=lists,
        config=config,
        queue=args.queue,
        dir=os.getcwd(),
        sys=args.sys,
        buildno=args.build,
        dry_run=args.dry_run
    )

submit_task = collections.namedtuple("submit_task", [
    "jobid",
    "subjobid",
    "jobname",
    # "cmd",
    "stdout",
    "stderr",
    "exe",
    "W",
    "app",
    "R",
    "queue",
    "dry"
])
if __name__ == '__main__':
    main()



