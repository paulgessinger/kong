from __future__ import print_function

DATEFORMAT = "%Y-%m-%d %H:%M:%S"

class BatchSystem:
    # def __init__(self):
        # raise NotImplementedError()

    # ATLASIO_HIGH = "ATLASIO_HIGH"
    # ATLASIO_LOW = "ATLASIO_LOW"


    def get_jobs_info(self):
        raise NotImplementedError()

    def submit(self, command, name, queue, walltime, stdout, stderr, extraopts=[], dry=False):
        # def bsub(command, queue, W, app, R, name, stdout, stderr, dry=False):
        raise NotImplementedError()

    def kill(self, jobid):
        raise NotImplementedError()

    def resubmit(self, jobid):
        raise NotImplementedError()

    def remove(self, jobid, subjobid, batchjobid):
        raise NotImplementedError()

    def get_opts_node():
        raise NotImplementedError()


class BatchJob:
    
    class Status:
        PENDING = "PEND"
        RUNNING = "RUN"
        EXIT = "EXIT"
        DONE = "DONE"
        UNKNOWN = "UNKWN"

    def __init__(self, jobid, status, name, exec_host, queue=None, extraopts=[]):
        self.name = name
        self.queue = queue
        self.exec_host = exec_host
        self.extraopts = extraopts
        self.jobid = jobid
        self.status = status
        

class Walltime:
    def __init__(hours=0, minutes=0, days=0):
        self.hours = hours
        self.days = days
        self.minutes = minutes

class BatchJobInfoFile:
    def __init__(self, path):
        with open(path) as f:
            lines = f.readlines()

        self.infodict = {}
        for l in lines:
            k, v = l.split(":", 1)
            k = k.strip()
            v = v.strip()
            self.infodict[k] = v

    def __getattr__(self, key):
        if not key in self.infodict:
            return None
        return self.infodict[key]

    def __str__(self):
        s = []
        for k, v in self.infodict.iteritems():
            s.append("{}={}".format(k, v))
        return "BatchJobInfoFile({})".format(", ".join(s))

