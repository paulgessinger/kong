from __future__ import print_function
import subprocess
import os
import sys
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
import xml.etree.ElementTree as ET

from . import BatchSystem
from . import Walltime
from . import BatchJob

class SGE(BatchSystem):
    def __init__(self, workdir):
        self.workdir = workdir
        self.jobinfodir = os.path.join(workdir, "jobinfo")
        self.jobscriptsdir = os.path.join(workdir, "jobscripts")

    def parse_job_xml(self, xml):
        name = xml.find("JB_name").text
        state = xml.find("state").text
        runinf = xml.find("queue_name").text
        
        if runinf and "@" in runinf:
            queue, host = runinf.split("@")
        else:
            queue, host = None, None

        jobid = xml.find("JB_job_number").text

        p = BatchJob.Status.PENDING
        e = BatchJob.Status.EXIT
        r = BatchJob.Status.RUNNING
        d = BatchJob.Status.DONE
        u = BatchJob.Status.UNKNOWN

        if state == "qw":
            status = p
        elif state == "t":
            status = p
        elif state == "r": 
            status = r
        elif state == "Eqw":
            status = p
        elif "E" in state: 
            status = e
        return name, status, jobid, queue, host

    def get_job_info(self):

        logger.debug("Batch info read from actual output")
        out = subprocess.check_output(["qstat", "-xml"])

        info = ET.fromstring(out)
        jobs = []
        
        for child in info:
            # print(child.tag)
            if child.tag == "queue_info":
                for jl in child:
                    jobs.append(self.parse_job_xml(jl))
            elif child.tag == "job_info":
                for jl in child:
                    jobs.append(self.parse_job_xml(jl))


        # for j in jobs:
            # print(j)


        batchjobs = []
        for name, status, jobid, queue, host in jobs:
            batchjobs.append(BatchJob(
                jobid = jobid,
                name = name[2:],
                queue = queue,
                exec_host = host,
                status = status
            ))


        return batchjobs


    def submit(self, jobid, subjobid, command, name, queue, walltime, stdout, stderr, extraopts=[], dry=False):
        
        cmd = [
            "qsub",
            # "-W", walltime,
            # "-q", queue,
        ]
        
        # if R != None:
            # cmd += ["-R", R]
        
        cmd += [
            "-N", "k_"+name,
            "-e", stderr,
            "-o", stdout,
        ]

        cmd += extraopts

        scriptfile = os.path.join(self.jobscriptsdir, "{}_{}.sh".format(jobid, subjobid))
        job_info_path = os.path.join(self.jobinfodir, "$JOB_ID.txt")

        cmd.append(scriptfile)

        logger.debug(" ".join(cmd))


        # create both paths
        for p in (os.path.dirname(scriptfile), os.path.dirname(job_info_path)):
            logger.debug("Creating dir {}".format(p))
            os.system("mkdir -p {}".format(p))

        dateformat = "%Y-%m-%d %H:%M:%S"
        script_body = [
            '#!/bin/bash',
            'function aborted() {',
            '  echo Aborted with signal $1.',
            '  echo "signal: $1" >> {}'.format(job_info_path),
            '  echo "end_time: $(LC_ALL=en_US.utf8 date \'+{}\')" >> {}'.format(dateformat, job_info_path),
            '  exit -1',
            '}',
            # 'mkdir -p %s' % self.,
            'for sig in SIGHUP SIGINT SIGQUIT SIGTERM SIGUSR1 SIGUSR2; do trap "aborted $sig" $sig; done',
            'echo "hostname: $HOSTNAME" > {}'.format(job_info_path),
            'echo "batchjobid: $JOB_ID" >> {}'.format(job_info_path),
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

            # print(quote(cmd))
            try:
                output = subprocess.check_output(cmd)
                batchid = output.split(" ")[2]
                return batchid
            except subprocess.CalledProcessError as e:
                logger.error(str(e), e.output)
                raise
        else:
            return 42

    def kill(self, jobid):
        cmd = ["qdel", jobid]
        logger.debug(" ".join(cmd))
        try:
            out = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            logger.warning(str(e))

    def resubmit(self, jobid, mode):
        raise NotImplementedError()
    
    def remove(self, jobid, subjobid, batchjobid):
        scriptfile = os.path.join(self.workdir, "jobscripts", "{}_{}.sh".format(jobid, subjobid))
        job_info = os.path.join(self.workdir, "jobinfo", "{}.txt".format(batchjobid))
        for p in (scriptfile, job_info):
            if os.path.exists(p):
                os.remove(p)


