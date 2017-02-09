from __future__ import print_function
import subprocess
import os
import stat
import re
import datetime
# from datetime import timedelta
import time
from log import logger
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

from . import BatchSystem
from . import Walltime
from . import BatchJob

class LSF(BatchSystem):
    def __init__(self, workdir):
        self.workdir = workdir
        self.jobinfodir = os.path.join(workdir, "jobinfo")
        self.jobscriptsdir = os.path.join(workdir, "jobscripts")

    def get_job_info(self):
        delimiter="\7"

        logger.debug("bjobs info read from actual output")
        keys = ("jobid", "stat", "queue", "exec_host", "job_name", "submit_time", "cmd")
        out = subprocess.check_output(["bjobs", "-a", "-o", " ".join(keys)+" delimiter='"+delimiter+"'"])

        lines = out.split("\n")
        head = [l.lower() for l in lines[0].split(delimiter)]
        jobs = lines[1:-1]

        jobout = []

        batchjobs = []

        for raw in jobs:
            job = dict(zip(head, raw.split(delimiter)))
            # jobout.append(job)
           
            try: 
                status = {
                    "PEND": BatchJob.Status.PENDING,
                    "RUN": BatchJob.Status.RUNNING,
                    "EXIT": BatchJob.Status.EXIT,
                    "DONE": BatchJob.Status.DONE,
                }[job["stat"]]
            except:
                status = BatchJob.Status.UNKNOWN

            # print(job)
            batchjobs.append(BatchJob(
                jobid = job["jobid"],
                # command = job["command"],
                name = job["job_name"],
                queue = job["queue"],
                # submit_time = job["submit_time"],
                exec_host = job["exec_host"],
                status = status
            ))


        return batchjobs


    def submit(self, jobid, subjobid, command, name, queue, walltime, stdout, stderr, extraopts=[], dry=False):
        
        cmd = [
            "bsub",
            "-W", walltime,
            "-q", queue,
        ]
        
        # if R != None:
            # cmd += ["-R", R]
        
        cmd += [
            "-J", name,
            "-eo", stderr,
            "-oo", stdout,
        ]

        cmd += extraopts

        # cmd += [command]

        scriptfile = os.path.join(self.jobscriptsdir, "{}_{}.sh".format(jobid, subjobid))
        job_info_path = os.path.join(self.jobinfodir, "$LSB_JOBID.txt")

        cmd.append(scriptfile)

        logger.debug(" ".join(cmd))


        # create both paths
        for p in (os.path.dirname(scriptfile), os.path.dirname(job_info_path)):
            logger.debug("Creating dir {}".format(p))
            os.system("mkdir -p {}".format(p))

        dateformat = "%Y-%m-%d %H:%M:%S"
        script_body = [
            'function aborted() {',
            '  echo Aborted with signal $1.',
            '  echo "signal: $1" >> {}'.format(job_info_path),
            '  echo "end_time: $(LC_ALL=en_US.utf8 date \'+{}\')" >> {}'.format(dateformat, job_info_path),
            '  exit -1',
            '}',
            # 'mkdir -p %s' % self.,
            'for sig in SIGHUP SIGINT SIGQUIT SIGTERM SIGUSR1 SIGUSR2; do trap "aborted $sig" $sig; done',
            'echo "hostname: $HOSTNAME" > {}'.format(job_info_path),
            'echo "batchjobid: $LSB_JOBID" >> {}'.format(job_info_path),
            'echo "submit_time: {}" >> {}'.format(datetime.datetime.now().strftime(dateformat), job_info_path),
            'echo "start_time: $(LC_ALL=en_US.utf8 date \'+{}\')" >> {}'.format(dateformat, job_info_path)
        ]

        script_body += [
            "",
            "# PAYLOAD COMMAND:",
            command,
            "# END PAYLOAD COMMAND",
            "",
        ]

        script_body += [
            'exit_status=$?',
            'echo "exit_status: $exit_status" >>%s' % job_info_path,
            'echo "end_time: $(LC_ALL=en_US.utf8 date \'+{}\')" >> {}'.format(dateformat, job_info_path),
            'exit $exit_status',
        ]

        if logger.getEffectiveLevel() <= 10:
            print("\n".join(script_body))
            print(" ".join(cmd))

        # print(scriptfile)
        # print("\n".join(script_body))

        if not dry:
            with open(scriptfile, "w+") as f:
                f.write("\n".join(script_body))

            # make executable
            st = os.stat(scriptfile)
            os.chmod(scriptfile, st.st_mode | stat.S_IEXEC)

            output = subprocess.check_output(cmd)
            bsub_regex = re.compile("Job <(.*?)>")
            lsf_id = bsub_regex.findall(output)[0]
            return lsf_id
        else:
            return 42

    def kill(self, jobid):
        cmd = ["bkill", jobid]
        logger.debug(" ".join(cmd))
        try:
            out = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            logger.warning(str(e))

    def resubmit(self, jobid, mode=""):
        cmd = ["brequeue"] 
        
        if len(mode) > 0:
            cmd.append("-"+mode)

        cmd.append(jobid)
        
        out = subprocess.check_output(cmd)

    def remove(self, jobid, subjobid, batchjobid):
        scriptfile = os.path.join(self.workdir, "jobscripts", "{}_{}.sh".format(jobid, subjobid))
        job_info = os.path.join(self.workdir, "jobinfo", "{}.txt".format(batchjobid))
        for p in (scriptfile, job_info):
            logger.debug("Remove file at {}".format(p))
            if os.path.exists(p):
                os.remove(p)




# def bmod(lsfid, queue=None, W=None):
    # cmd = ["bmod", lsfid]
    # if queue != None:
        # cmd.append("-q")
        # cmd.append(queue)
    # if W != None:
        # cmd.append("-W")
        # cmd.append(W)

    # out = subprocess.check_output(cmd)

