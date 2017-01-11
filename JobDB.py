from __future__ import print_function

import os
import sqlite3
# import subprocess
import lsf
from log import logger
import datetime
import time



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

        logger.debug("Syncing to database")


        for job in jobs:
            # print("syncing", job)

            # figure out kong job id
            jobid, _ = job["job_name"].split("_", 1)
            jobid = int(jobid)

            values = (
                jobid,
                job["stat"],
                job["job_name"],
                job["queue"],
                job["submit_time"],
                job["exec_host"],
                job["command"],
                job["jobid"],
            )
            # print(values)
            stmt = '''UPDATE jobs SET
                jobid = ?,
                stat = ?,
                job_name = ?,
                queue = ?,
                submit_time = ?,
                exec_host = ?,
                cmd = ?
            WHERE batchjobid = ?;'''
            # print(stmt)
            filec.execute(stmt, values)
            # memc.execute(stmt, values)
            if filec.rowcount == 0:
                # this did not affect anything, insert!
                values = (
                    jobid,
                    job["jobid"],
                    job["stat"],
                    job["job_name"],
                    job["queue"],
                    job["submit_time"],
                    job["exec_host"],
                    job["command"],
                )
                stmt = '''INSERT INTO jobs (jobid, batchjobid, stat, job_name, queue, submit_time, exec_host, cmd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''
                filec.execute(stmt, values)
                # memc.execute(stmt, values)
                # logger.debug("Inserting {}".format(job["jobid"]))

        self.fileconn.commit()

        logger.debug("Syncing completed")
        os.system("touch {}".format(self.validfile))
        # print(jobs)

    def remove(self, jobid):
        c = self.fileconn.cursor()
        c.execute("DELETE FROM jobs WHERE jobid = ?", (jobid,))

    def register_subjob(self, jobid, subjobid, lsfid):
        c = self.fileconn.cursor()
        c.execute('INSERT INTO jobs (jobid, subjobid, batchjobid) VALUES (?, ?, ?)', (jobid, subjobid, lsfid))
        # self.fileconn.commit()

    def commit(self):
        self.fileconn.commit()

    def get_jobinfo_batch(self, lsfids):
        return {}
