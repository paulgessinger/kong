from __future__ import print_function
import subprocess
import os
import re
import datetime
import time
from log import logger

BJOBS_CACHE_LIFETIME = 30

bsub_regex = re.compile("Job <(.*?)>")

def bsub(exe, queue, W, app, R, name, stdout, stderr, dry=False):
    # cmd = "bsub -W {W} -app {app} -q {q} -R \"{R}\" -J {name} -eo {eo} -oo {oo} \"{exe}\"".format(exe=exe, q=queue, W=W, app=app, R=R, oo=stdout, eo=stderr, name=name)
    cmd = [
        "bsub",
        "-W", str(W),
        "-app", app,
        "-q", queue,
        "-n", "1"
    ]
    
    if R != None:
        cmd += ["-R", R]
    
    cmd += [
        "-J", name,
        "-eo", stderr,
        "-oo", stdout,
        exe
    ]

    logger.debug(" ".join(cmd))

    if not dry:
        output = subprocess.check_output(cmd)
        lsf_id = bsub_regex.findall(output)[0]
        return lsf_id
    else:
        # time.sleep(0.05)
        return 42

def bkill(jobid):

    cmd = ["bkill", jobid]
    logger.debug(" ".join(cmd))
    try:
        out = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as e:
        logger.warning(str(e))

def bjobs_raw(delimiter="\7"):
    # if os.path.exists(cachefile):
    #     mtime = os.path.getmtime(cachefile)
    #     now = time.mktime(datetime.datetime.now().timetuple())
    #     if now-mtime < BJOBS_CACHE_LIFETIME:
    #         with open(cachefile, "r") as f:
    #             data = f.read()
    #         logger.debug("bjobs info read from cachefile")
    #         return data
   
    logger.debug("bjobs info read from actual output")
    keys = ("jobid", "stat", "queue", "exec_host", "job_name", "submit_time", "cmd")
    out = subprocess.check_output(["bjobs", "-a", "-o", " ".join(keys)+" delimiter='"+delimiter+"'"])
   
    # with open(cachefile, "w+") as f:
    #     f.write(out)

    return out

zipped_jobs_cache = None
zipped_jobs_cache_timestamp = time.mktime(datetime.datetime.now().timetuple())

def bjobs(delimiter="\7"):
    out = bjobs_raw(delimiter)

    lines = out.split("\n")
    head = [l.lower() for l in lines[0].split(delimiter)]
    jobs = lines[1:-1]

    jobout = []

    for raw in jobs:
        job = dict(zip(head, raw.split(delimiter)))
        jobout.append(job)


    return jobout

# def get_job_info(cachefile, jobs_query=[]):
#     delimiter = "\7"
#     out = bjobs_raw(cachefile, delimiter)
#
#     lines = out.split("\n")
#     head = [l.lower() for l in lines[0].split(delimiter)]
#     jobs = lines[1:-1]
#
#     global zipped_jobs_cache
#     global zipped_jobs_cache_timestamp
#     now = time.mktime(datetime.datetime.now().timetuple())
#
#     # print(now-zipped_jobs_cache_timestamp)
#     if now-zipped_jobs_cache_timestamp > BJOBS_CACHE_LIFETIME:
#         zipped_jobs_cache = None
#         zipped_jobs_cache_timestamp = time.mktime(datetime.datetime.now().timetuple())
#
#     if zipped_jobs_cache == None:
#         zipped_jobs_cache = {}
#         for raw in jobs:
#             job = dict(zip(head, raw.split(delimiter)))
#             zipped_jobs_cache[job["jobid"]] = job
#
#     if len(jobs_query) == 0:
#         return zipped_jobs_cache
#
#     outjobs = []
#     for reqj in jobs_query:
#         if reqj in zipped_jobs_cache:
#             outjobs.append(zipped_jobs_cache[reqj])
#
#     return outjobs


def brequeue(lsfid, mode):
    cmd = ["brequeue"] 
    
    if len(mode) > 0:
        cmd.append("-"+mode)

    cmd.append(lsfid)
    
    out = subprocess.check_output(cmd)

def bmod(lsfid, queue=None, W=None):
    cmd = ["bmod", lsfid]
    if queue != None:
        cmd.append("-q")
        cmd.append(queue)
    if W != None:
        cmd.append("-W")
        cmd.append(W)

    out = subprocess.check_output(cmd)













