#! /usr/bin/env python

from __future__ import print_function
import os
import sys
import argparse
from ConfigParser import SafeConfigParser
import fcntl
from contextlib import contextmanager
import datetime
from termcolor import colored
import shutil
import logging
import collections
import multiprocessing as mp
import threading as th

from FileSplitterUtils import splitFileListBySizeOfSubjob
from lsf import *
from python_utils.printing import *
from log import logger

sys.path.append('/project/atlas/software/python_include/')
from SUSYHelpers import *

__all__ = ["submit"]


JOBOUTDIR_FORMAT = "{jobid:05d}_{jobname}"
JOBSTDOUT_FORMAT = "{jobid:05d}_{subjobid:05d}.stdout"
JOBSTDERR_FORMAT = "{jobid:05d}_{subjobid:05d}.stderr"

CONFIG_TEMPLATE = """
[kong]
kongdir={kongdir}
registry={regdir}
output={outdir}

[analysis]
# output=/etapfs02/atlashpc/pgessing/output/
# framework_job_script=/gpfs/fs1/home/pgessing/workspace_xAOD/AnalysisJob/runGenericJobMogon.py
# input_tarball=/home/pgessing/workspace_xAOD/input_tarballs//input.tar
# binary=./Analysis
# algo=AlgoWPR

"""


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

    if not os.path.exists(config_file):
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
    
    return cp

def truncate_middle(str, length):
    if len(str) < length:
        return str.ljust(length)
    
    part1 = str[:length/2-3]
    part2 = str[len(str)-(length/2-2):]

    outstr = (part1 + "(...)" + part2).ljust(length)
    return outstr

def get_directories(config):
    kongdir = config.get("kong", "kongdir")
    regdir = config.get("kong", "registry")
    outdir = config.get("kong", "output")
    return (kongdir, regdir, outdir)

def get_tty_width():
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
    lsfids = []

    for f in contents:
        fullf = os.path.join(dir, f)
        
        if os.path.isdir(fullf):
            sub = sum_up_directory(fullf)
            lsfids += sub
            continue
        
        if not os.path.isfile(fullf) or not f.endswith(".job"):
            continue
        

        with openlock(fullf, "r") as jobf:
            subjobs = jobf.read()

        subjobs = subjobs.split("\n")[:-1]
        for subjob in subjobs:
            _, lsfid = subjob.split(":")
            lsfids.append(lsfid)

    # print(lsfids)
    return lsfids

def make_status_string(jobs):
    npend = 0
    ndone = 0
    nrun = 0
    nexit = 0
    nother = 0
    ntotal = len(jobs)


    for j in jobs:
        # print(j)
        if j["stat"] == "DONE": ndone +=1
        elif j["stat"] == "PEND": npend +=1
        elif j["stat"] == "EXIT": nexit +=1
        elif j["stat"] == "RUN": nrun +=1
        else: nother += 1

    ngone = ntotal - (ndone + npend + nexit + nrun + nother)
   
    status_string = "{p:>4d} P | {r:>4d} R | {d:>4d} D | {e:>4d} E | {o:>4d} O".format(p=npend, d=ndone, r=nrun, e=nexit, o=nother)
    
    color = "white"
    if nexit > 0:
        color = "red"
    elif npend > 0:
        color = "yellow"
    elif nrun > 0:
        color = "blue"
    elif ndone == ntotal:
        color = "green"

    return (status_string, color)

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

def submit(lists, config=None, dir=None, verbosity=None, dry_run=False):
    """
    Input format for lists is a list of tuples with (NAME, LISTFILE).
    """

    if verbosity != None:
        if verbosity > 0:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    if len(lists) == 0:
        logger.error("Lists input was empty")
        raise ValueError()

    if not config:
        config = get_config()

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

    submit_dir = dir if dir else regdir
    if not os.path.isabs(submit_dir):
        submit_dir = os.path.join(regdir, dir)
    
    logger.info("Submitting to {}".format(submit_dir))
    
    name_list_splits = [(n, l, splitFileListBySizeOfSubjob(l, 5.5)) for n, l in lists]

    analysis_outdir = config.get("analysis", "output")
    input_tarball = config.get("analysis", "input_tarball")
    binary = config.get("analysis", "binary")
    algo = config.get("analysis", "algo")

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
            jobfile = open(os.path.join(submit_dir, "{:05d}_{}.job".format(jobid, jobName)), "w")
            jobfiles[jobid] = jobfile

        

        for split in splits:
            subjobid +=1
            stdoutfile = os.path.join(outdir, JOBSTDOUT_FORMAT.format(jobid=jobid, subjobid=subjobid))
            stderrfile = os.path.join(outdir, JOBSTDERR_FORMAT.format(jobid=jobid, subjobid=subjobid))
           
            cmd = [
                config.get("analysis", "framework_job_script"), 
                '--binary', binary, 
                '--composedOptions', '\'{algo} -p 0. -n 0 --wn --wnDir . \''.format(algo=algo),
                '--filelist', list, 
                '--outputMask', 'Plots\\*.root', 
                '--outputDir', joboutdir,
                '--listRanges', split,
                '--inputTarballName', input_tarball, 
                '--jobid', str(jobid), 
                '--subjobid', str(subjobid), 
                '--jobname', jobName,
                '--analysisRelease', 'Base,2.4.18', 
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
                app="Reserve5G", 
                R="rusage[atlasio=10]"+blacklist_string, 
                queue="atlasshort",
                dry=dry_run
            ))

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

    if not dry_run:
        logger.debug("closing jobfile handles")
        for key, f in jobfiles.iteritems():
            f.close()

    jobcachefile = os.path.join(kongdir, "bjobs_cache")
    logger.debug("killing bjobs cache file {}".format(jobcachefile))
    if not dry_run and os.path.exists(jobcachefile):
        os.remove(jobcachefile)

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

def thread_map(func, values, threadcount=4, tick=lambda x:x, tick_throttle=0.1):
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


#################
# CLI FUNCTIONS #
#################

def main():
    """
    kong submits jobs to LSF and keeps track of them, with a directory hierarchy of .job files
    """
    
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

    submitp = subparsers.add_parser("submit", parents=[parentp], help=cli_submit.__doc__)
    submitp.set_defaults(func=cli_submit)
    submitp.add_argument("lists", nargs="+", help="The lists to submit. You can glob in the shell")
    submitp.add_argument("--name", help="Name to assign to the job. Only works when a single list is given")
    submitp.add_argument("--dry-run", action="store_true", help="Don't really do anything")

    lsp = subparsers.add_parser("ls", parents=[parentp], help=ls.__doc__)
    lsp.set_defaults(func=ls)
    lsp.add_argument("dir", nargs="?", default=os.getcwd(), help="The directory or files to list. You can glob")
    lsp.add_argument("--all", "-a", action="store_true", help="If given, list all jobs found in the registry")
    lsp.add_argument("--force", "-f", action="store_true", help="Kill the bjobs cache")

    rmp = subparsers.add_parser("rm", parents=[parentp], help=rm.__doc__)
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
    resubmitp.add_argument("--status", "-s", choices=["done", "pend", "exit", "run"], default="exit", help="Specify the status with which jobs qualify to be resubmitted")
    resubmitp.add_argument("--force", "-f", action="store_true", help="Kill the the bjobs cache file before doing anything")

    viewp = subparsers.add_parser("view", parents=[parentp], help=view.__doc__)
    viewp.set_defaults(func=view)
    viewp.add_argument("tgt", nargs="+", help=JOB_RANGE_HELP)
    viewp.add_argument("--force", "-f", action="store_true", help="Kill the bjobs cache")
    viewp.add_argument("--status", "-s", choices=["done", "pend", "exit", "run"], help="Only show subjobs with this status")
    

    args = parser.parse_args()

    log_level = logging.INFO
    if args.verbose > 0:
        log_level = logging.DEBUG
    # elif args.verbose > 1:
        # log_level = logging.DEBUG

    logger.setLevel(log_level)

    config = get_config()

    args.func(args, config)

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
    
    jobcachefile = os.path.join(kongdir, "bjobs_cache")
    if args.force and os.path.exists(jobcachefile):
        os.remove(jobcachefile)
    
    job_range = parse_job_range_list(tgt)
    all_jobfiles = find_job_files(regdir)

    job_info = get_job_info(jobcachefile)

    selected = []

    for jobid in job_range:
        if not jobid in all_jobfiles:
            logger.warning("job with id {} is not found in registry".format(jobid))
            continue

        path = all_jobfiles[jobid]
        
        with openlock(path, "r") as f:
            for l in f.read().split("\n")[:-1]:
                subjobid, subjoblsfid = l.split(":")
                lsfjob = job_info[subjoblsfid]
                stat = lsfjob["stat"]
                if stat == args.status.upper():
                    selected.append(subjoblsfid)

    if not confirm("Resubmitting the following {} LSF jobs: {}".format(len(selected), ", ".join(selected))):
        return
    # print(selected) 
    for s in selected:
        mode = args.status[0].lower()
        # print(mode, s)
        brequeue(s, mode)

    if os.path.exists(jobcachefile):
        os.remove(jobcachefile)
    


def view(args, config):
    """
    Show the subjobs and their statuses for a job range
    """

    tgt = args.tgt
    kongdir, regdir, outdir = get_directories(config)
    analysis_output = config.get("analysis", "output")
    
    jobcachefile = os.path.join(kongdir, "bjobs_cache")
    if args.force and os.path.exists(jobcachefile):
        os.remove(jobcachefile)
    
    job_range = parse_job_range_list(tgt)
    all_jobfiles = find_job_files(regdir)

    job_info = get_job_info(jobcachefile)
    # lsfids = []
    # subjobs = []

    width = get_tty_width()

    for jobid in job_range:
        if not jobid in all_jobfiles:
            logger.warning("job with id {} is not found in registry".format(jobid))
            continue

        path = all_jobfiles[jobid]
        
        print("JOB", jobid)
        with openlock(path, "r") as f:
            for l in f.read().split("\n")[:-1]:
                subjobid, subjoblsfid = l.split(":")
                lsfjob = job_info[subjoblsfid]
                # print(subjobid, subjoblsfid, lsfjob["stat"])

                stat = lsfjob["stat"]

                if args.status and stat != args.status.upper():
                    continue

                outstr = "{:>5} | {:>8} | {}".format(subjobid, subjoblsfid, stat.ljust(4))


                color = "white"
                if stat == "PEND":
                    color = "yellow"
                elif stat == "RUN":
                    color = "blue"
                elif stat == "DONE":
                    color = "green"
                elif stat == "EXIT":
                    color = "red"

                print(colored(outstr, color))
                # subjobs.append((jobid, subjobid, subjoblsfid))
                # lsfids.append(subjoblsfid)
                
        

    # subjobinfo = get_job_info(jobcachefile, lsfids)

        

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
        
        with openlock(path, "r") as f:
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
    if os.path.exists(jobcachefile):
        os.remove(jobcachefile)

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


def ls(args, config):
    """
    Show information over job files or directory hierarchies with job files
    """

    kongdir, regdir, outdir = get_directories(config)
    jobcachefile = os.path.join(kongdir, "bjobs_cache")

    if args.dir == "*":
        return
    
    if args.force:
        if os.path.exists(jobcachefile):
            os.remove(jobcachefile)

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

    width = get_tty_width()

    for subdir in dirs:
        lsfids = sum_up_directory(subdir)
        if len(lsfids) == 0:
            continue
        subjobinfo = get_job_info(jobcachefile, lsfids)
        status_string, color = make_status_string(subjobinfo)
        outstr = " "*6 + "| {name} | {status}".format(
            name="{name}",
            status=status_string
        )
       
        name = os.path.relpath(subdir, dir)
        outstr = outstr.format(name=truncate_middle(name, width - len(outstr) + len("{name}")))
        print(colored(outstr, color))



    for fullf in jobfiles:
        f = os.path.basename(fullf)

        jobid, name = f[:-4].split("_", 1)
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fullf))

        with open(fullf, "r") as jobf:
            subjobs = jobf.read().split("\n")[:-1]

        subjobids = []
        for sji in subjobs:
            i, lsfid = sji.split(":")
            subjobids.append(lsfid)
        
        if args.all:
            bd = os.path.relpath(os.path.dirname(fullf), regdir)
            name = bd + "/" + name

        subjobinfo = get_job_info(jobcachefile, subjobids)
        status_string, color = make_status_string(subjobinfo)

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
        print(colored(outstr, color))



def peek(args, config):
    """
    Show the stdout or stderr of a given subjob
    """

    kongdir, regdir, outdir = get_directories(config)

    f = os.path.join(outdir, "{:05d}_{:05d}.std{}".format(args.jobid, args.subjobid, args.stream))

    os.system("less {}".format(f))



def cli_submit(args, config):
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

    submit(
        lists=lists,
        config=config,
        dir=os.getcwd(),
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



