from __future__ import print_function

import os
import sys
import sqlite3
# import subprocess
from log import logger
import datetime
import time
from tqdm import tqdm
from batch import BatchSystem, BatchJobInfoFile, BatchJob, DATEFORMAT
from traceback import print_tb



class JobDB:

    def __init__(self, dbfile, timeout, backend):
        assert isinstance(backend, BatchSystem)
        self.backend = backend
        self.dbfile = dbfile
        self.validfile = os.path.join(os.path.dirname(self.dbfile), "kong_db_valid")
        if not os.path.exists(self.dbfile):
            self.fileconn = sqlite3.connect(self.dbfile)
            self.setup()
        else:
            self.fileconn = sqlite3.connect(self.dbfile)


        self.cache_timeout = timeout
        self.fileconn.row_factory = sqlite3.Row

        self.memconn = sqlite3.connect(":memory:")
        self.memconn.row_factory = sqlite3.Row

        self.load()

    def invalidate(self):
        if os.path.exists(self.validfile):
            os.remove(self.validfile)

    def load(self):
        # print("BEGIN LOAD")
        filedb = self.fileconn
        memdbc = self.memconn.cursor()

        # for line in filedb.iterdump():
        #     memdbc.executescript(line)
        # self.memconn.commit()
        # print("END LOAD")

    def setup(self):
        c = self.fileconn.cursor()
        c.execute('''CREATE TABLE jobs (
            jobid INTEGER,
            subjobid INTEGER,
            batchjobid VARCHAR PRIMARY KEY,
            stat VARCHAR,
            job_name VARCHAR,
            queue VARCHAR,
            walltime VARCHAR,
            submit_time VARCHAR,
            start_time VARCHAR,
            end_time VARCHAR,
            signal VARCHAR,
            exec_host VARCHAR,
            cmd TEXT
        )''')
        c.execute('''CREATE INDEX jobs_jobid ON jobs (jobid);''')
        c.execute('''CREATE INDEX jobs_batchjobid ON jobs (batchjobid);''')
        self.fileconn.commit()

    def getBatchJob(self, batchjobid):
        c = self.fileconn.cursor()
        c.execute('SELECT * FROM jobs WHERE batchjobid = ?', (jobid,))
        return c.fetchone()

    def getBatchJobsForJob(self, jobid):
        c = self.fileconn.cursor()

        c.execute('SELECT * FROM jobs WHERE jobid = ? ORDER BY subjobid ASC', (jobid,))
        # return sorted(c.fetchall(), key=lambda r: int(r["subjobid"]))
        return c.fetchall()
    def getJobInfo(self, jobid):
        c = self.fileconn.cursor()


        c.execute('SELECT COUNT(*) FROM jobs WHERE jobid = ?', (jobid,))
        total = c.fetchone()[0]

        res = [total]

        found = 0

        for status in ["pend", "done", "run", "exit"]:
            # logger.debug("Counting {} for job {}".format(status, jobid))
            c.execute('SELECT COUNT(*) as num FROM jobs WHERE jobid = ? and stat = ?', (jobid, status.upper()))
            row = c.fetchone()
            found += row[0]
            res.append(row[0])

        res.append(total-found)

        return res

    def cleanup(self):
        logger.debug("Cleaning up")

        jobinfodir = self.backend.jobinfodir
        c = self.fileconn.cursor()
        for j in os.listdir(jobinfodir):
            jf = os.path.join(jobinfodir, j)
            jobid = j[:-4]
            if not os.path.isfile(jf): continue
            ji = BatchJobInfoFile(jf)
            # print(ji)
            # print(jobid, ji.exit_status != None, ji.exit_status == "0", ji.end_time != None)
            now = datetime.datetime.now()
            if ji.exit_status != None and ji.exit_status == "0" and ji.end_time != None:
                # looks like this is done. how long?
                end = datetime.datetime.strptime(ji.end_time, DATEFORMAT)
                delta = now-end
                # print(delta.days)
                if delta.days > 2:
                    logger.debug("{} is older than 2 days ({})".format(jobid, delta.days))
                    logger.debug("Deleting {}".format(jf))
                    # print(ji.exit_status)
                    os.remove(jf)
                # print(end)

    def update(self, force=False):

        # check mod time
        if os.path.exists(self.validfile):
            mtime = os.path.getmtime(self.validfile)
            now = time.mktime(datetime.datetime.now().timetuple())
            # print(now-mtime)
            if now-mtime < self.cache_timeout and not force:
                logger.debug("Skip updating jobs db. age: {} < {}".format(now-mtime, self.cache_timeout))
                return

        filec = self.fileconn.cursor()
        
        # updating first from kong job info file
        jobinfodir = self.backend.jobinfodir
        logger.debug("Updating from internal job info files")
        
        jobinfofiles = os.listdir(jobinfodir)
        crit = 10000
        if len(jobinfofiles) > crit:
            logger.warning("Over {} job info files found ({}). You might want to run `kong cleanup`".format(crit, len(jobinfofiles)))

        for j in tqdm(jobinfofiles, leave=False, desc="Updating from internal files", bar_format="{l_bar}{bar}|{n_fmt}/{total_fmt}"):
        # for j in os.listdir(jobinfodir):
            jf = os.path.join(jobinfodir, j)
            jobid = j[:-4]
            if not os.path.isfile(jf): continue
            ji = BatchJobInfoFile(jf)
            # print(ji)
            logger.debug("Updating job {}".format(jobid))

            columns = []
            values = []

            if ji.signal:
                columns.append("stat")
                values.append(BatchJob.Status.EXIT)
            elif ji.exit_status:
                columns.append("stat")
                if ji.exit_status == "0":
                    values.append(BatchJob.Status.DONE)
                else:
                    values.append(BatchJob.Status.EXIT)
            elif ji.start_time:
                columns.append("stat")
                values.append(BatchJob.Status.RUNNING)

            if ji.end_time:
                columns.append("end_time")
                values.append(ji.end_time)
            
            if ji.submit_time:
                columns.append("submit_time")
                values.append(ji.submit_time)
            
            if ji.start_time:
                columns.append("start_time")
                values.append(ji.start_time)

            if ji.hostname:
                columns.append("exec_host")
                values.append(ji.hostname)

            columns.append("signal")
            if ji.signal:
                values.append(ji.signal)
            else:
                values.append("")


            values.append(jobid)

            columns = [c + " = ?" for c in columns]

            stmt = '''UPDATE jobs SET\n{c}\nWHERE batchjobid = ?;'''

            stmt = stmt.format(c = ",\n".join(columns))

            # print(stmt, values)

            # logger.debug("Updating job {}".format(jobid))
            
            filec.execute(stmt, values)

        self.fileconn.commit()
        
        logger.debug("Completed, updating from internal job info files")
    
    
        jobs = self.backend.get_job_info()


        logger.debug("Syncing to database")


        for job in tqdm(jobs, leave=False, desc="Updating", bar_format="{l_bar}{bar}|{n_fmt}/{total_fmt}"):
            try:

                # figure out kong job id
                
                
                # parse jobid and subjobid from name
                if job.name.count("_") < 2:
                    logger.info("Unable to update job {} ({}). Probably submitted from outside".format(job.jobid, job.name))
                    continue

                jobid, parsed_subjobid, _ = job.name.split("_", 2)
                jobid, parsed_subjobid = int(jobid), int(parsed_subjobid)
                # print(parsed_jobid, parsed_subjobid)
                
                # assert jobid == parsed_jobid, "Jobid != Jobid in job name {}, {}".format(jobid, parsed_jobid)

                columns = [
                    "jobid",
                    "subjobid",
                    "stat",
                    "job_name",
                    "exec_host",
                ]
                values = [
                    jobid,
                    parsed_subjobid,
                    job.status,
                    job.name,
                    job.exec_host,
                ]

                if job.queue != None:
                    columns.append("queue")
                    values.append(job.queue)

                values.append(job.jobid)

                cols = [c+" = ?" for c in columns]

                stmt = '''UPDATE jobs SET\n{}\nWHERE batchjobid = ?;'''.format(",\n".join(cols))
                logger.debug("Updating job {}".format(job.jobid))
                
                filec.execute(stmt, values)
                if filec.rowcount == 0:
                    logger.debug("Not found, inserting")
                    # this did not affect anything, insert!
                    values = (
                        jobid,
                        parsed_subjobid,
                        job.jobid,
                        job.status,
                        job.name,
                        job.queue,
                        # job.submit_time,
                        job.exec_host,
                        # job.command,
                    )
                    stmt = '''INSERT INTO jobs (
                            jobid, 
                            subjobid, 
                            batchjobid, 
                            stat, 
                            job_name, 
                            queue,
                            exec_host
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)'''
                    filec.execute(stmt, values)

                logger.debug("Job updated / inserted")
            except BaseException as e:
                _, _, tb = sys.exc_info()
                # print(tb)
                print_tb(tb)
                logger.error(str(type(e))+" "+str(e))
        self.fileconn.commit()

        logger.debug("Syncing completed")
        os.system("touch {}".format(self.validfile))

    def remove(self, jobid):
        c = self.fileconn.cursor()
        c.execute("DELETE FROM jobs WHERE jobid = ?", (jobid,))

    def vacuum(self):
        c = self.fileconn.cursor()
        c.execute("VACUUM")

    def register_subjob(self, jobid, subjobid, batchid, cmd, queue, walltime):
        # print(jobid, subjobid, batchid, cmd, queue, walltime)
        c = self.fileconn.cursor()
        c.execute('INSERT INTO jobs (jobid, subjobid, batchjobid, cmd, queue, walltime) VALUES (?, ?, ?, ?, ?, ?)', (
            jobid,
            subjobid,
            batchid,
            cmd,
            queue,
            walltime,
        ))
        # self.fileconn.commit()

    def commit(self):
        self.fileconn.commit()

    def get_jobinfo_batch(self, lsfids):
        return {}
