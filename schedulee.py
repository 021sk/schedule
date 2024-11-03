import logging
import functools
import datetime

logger = logging.getLogger('schedule')


class ScheduleError(Exception):
    '''base schedule error'''


class ScheduleValueError(ScheduleError):
    '''base schedule value error'''


class IntervalError(ScheduleError):
    '''an improper interval was  used'''


class Scheduler():
    def __init__(self):
        self.jobs = []

    def every(self,interval):
        job = Job(interval,self)
        return job
    
    def run_pending(self):
        runnable_jobs = (job for job in self.jobs if job.should_run)
        for job in sorted(runnable_jobs):
            self._run_job(job)

    def _run_job(self,job):
        job.run()


class Job():
    def __init__(self,interval,scheduler):
        self.interval = interval
        self.scheduler = scheduler
        self.period = None
        self.last_run =None
        self.next_run = None
        self.job_func = None
        self.unit = None

    def __lt__ (self,other):
        return self.next_run < other.next_run


    @property
    def second(self):
        if self.interval != 1 :
            raise IntervalError("use seconds instead of second")
        return self.seconds
    
    @property
    def seconds(self):
        self.unit = 'seconds'
        return self

    @property
    def minute(self):
        if self.interval != 1 :
            raise IntervalError("use minutes instead of second")
        return self.seconds
    
    @property
    def minutes(self):
        self.unit = 'minutes'
        return self
    
    def do(self, job_func, *args, **kwargs):
        self.job_func = functools.partial(job_func, *args, **kwargs)
        functools.update_wrapper(self.job_func, job_func)
        self._schedule_next_run()
        if self.scheduler is None:
            raise ScheduleError(
                "unable to add job to schedule,"
                "job is not associated with an scheduler"
                )
        self.scheduler.jobs.append(self)
        return self
    
    def _schedule_next_run(self):
        if self.unit not in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
            raise ScheduleValueError("invalid unit")
        interval = self.interval

        self.period = datetime.timedelta(**{self.unit:interval})
        self.next_run = datetime.datetime.now() + self.period

    @property       
    def should_run(self):
        assert self.next_run is not None, "must run _schedule_next_run first"
        return datetime.datetime.now() >= self.next_run

    def run(self):
        logger.debug(f'running job {self}')
        ret = self.job_func()
        self.last_run = datetime.datetime.now()
        self._schedule_next_run()
        return ret

default_schedular = Scheduler()

def every(interval=1):
    return default_schedular.every(interval)

def run_pending():
    default_schedular.run_pending()
