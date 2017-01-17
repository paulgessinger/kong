from __future__ import print_function

import os
import sys
import sqlite3
# import subprocess
import lsf
from log import logger
import datetime
import time
from tqdm import tqdm



class JobDB:

    def __init__(self, dbfile, timeout):
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
            submit_time VARCHAR,
            exec_host VARCHAR,
            cmd TEXT
        )''')
        c.execute('''CREATE INDEX jobs_jobid ON jobs (jobid);''')
        c.execute('''CREATE INDEX jobs_batchjobid ON jobs (batchjobid);''')
        c.commit()

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

    def update(self, force=False):

        # check mod time
        if os.path.exists(self.validfile):
            mtime = os.path.getmtime(self.validfile)
            now = time.mktime(datetime.datetime.now().timetuple())
            # print(now-mtime)
            if now-mtime < self.cache_timeout and not force:
                logger.debug("Skip updating jobs db. age: {} < {}".format(now-mtime, self.cache_timeout))
                return

        jobs = lsf.bjobs()

        filec = self.fileconn.cursor()
        memc = self.memconn.cursor()

        # sys.stdout.write("Updating...\r")
        # sys.stdout.flush()

        logger.debug("Syncing to database")


        for job in tqdm(jobs, leave=False, desc="Updating", bar_format="{l_bar}{bar}|{n_fmt}/{total_fmt}"):
            # print("syncing", job)

            # figure out kong job id
            jobid, _ = job["job_name"].split("_", 1)
            jobid = int(jobid)
            
            # parse jobid and subjobid from name
            parsed_jobid, parsed_subjobid, _ = job["job_name"].split("_", 2)
            parsed_jobid, parsed_subjobid = int(parsed_jobid), int(parsed_subjobid)
            # print(parsed_jobid, parsed_subjobid)
            
            assert jobid == parsed_jobid, "Jobid != Jobid in job name {}, {}".format(jobid, parsed_jobid)

            values = (
                jobid,
                parsed_subjobid,
                job["stat"],
                job["job_name"],
                job["queue"],
                job["submit_time"],
                job["exec_host"],
                job["command"],
                job["jobid"],
            )


            stmt = '''UPDATE jobs SET
                jobid = ?,
                subjobid = ?,
                stat = ?,
                job_name = ?,
                queue = ?,
                submit_time = ?,
                exec_host = ?,
                cmd = ?
            WHERE batchjobid = ?;'''
            logger.debug("Updating job {}".format(job["jobid"]))
            # print(stmt)
            filec.execute(stmt, values)
            # memc.execute(stmt, values)
            if filec.rowcount == 0:
                logger.debug("Not found, inserting")
                # this did not affect anything, insert!
                values = (
                    jobid,
                    parsed_subjobid,
                    job["jobid"],
                    job["stat"],
                    job["job_name"],
                    job["queue"],
                    job["submit_time"],
                    job["exec_host"],
                    job["command"],
                )
                stmt = '''INSERT INTO jobs (
                        jobid, 
                        subjobid, 
                        batchjobid, 
                        stat, 
                        job_name, 
                        queue, 
                        submit_time, 
                        exec_host, 
                        cmd
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                filec.execute(stmt, values)
                # memc.execute(stmt, values)
                # logger.debug("Inserting {}".format(job["jobid"]))

            logger.debug("Job updated / inserted")
        self.fileconn.commit()

        logger.debug("Syncing completed")
        os.system("touch {}".format(self.validfile))
        # print(jobs)

    def remove(self, jobid):
        c = self.fileconn.cursor()
        c.execute("DELETE FROM jobs WHERE jobid = ?", (jobid,))

    def vacuum(self):
        c = self.fileconn.cursor()
        c.execute("VACUUM")

    def register_subjob(self, jobid, subjobid, lsfid):
        c = self.fileconn.cursor()
        c.execute('INSERT INTO jobs (jobid, subjobid, batchjobid) VALUES (?, ?, ?)', (jobid, subjobid, lsfid))
        # self.fileconn.commit()

    def commit(self):
        self.fileconn.commit()

    def get_jobinfo_batch(self, lsfids):
        return {}
