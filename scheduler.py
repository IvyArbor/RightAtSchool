import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

import sys
import os
from importlib import import_module
from settings import conf

executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}

class RightAtSchoolScheduler(object):
    instance = None

    def __init__(self, blocking=True):

        self.log = logging.getLogger('apscheduler.executors.default')
        if blocking is False:
            self.scheduler = BackgroundScheduler(
                executors=executors,
                timezone='utc')
        else:
            self.scheduler = BlockingScheduler(
                executors=executors,
                timezone='utc')


        print('Init RightAtSchool Scheduler')
        userhome = os.path.expanduser('~')
        self.user_name = os.path.split(userhome)[-1]

        self.initLogging()

        self.scheduler.add_job(self.integratePipeDrive, 'cron', day="1-31", hour=7)
        self.scheduler.add_job(self.integrateCypherWorx, 'cron', day="1-31", hour=8)
        self.scheduler.add_job(self.integrateQuickBooks, 'cron', day="1-31", hour=9)
        self.scheduler.add_job(self.integrateHR, 'cron', day="1-31", hour=10)
        self.scheduler.add_job(self.integrateActiveNet, 'cron', day="1-31", hour=11)

        self.scheduler.start()


    def initLogging(self):
        self.log.setLevel(logging.INFO)  # DEBUG
        formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.log.addHandler(handler)

    def runJob(self, job_name):
        class_name = job_name

        # Dynamically load the job class
        cls = getattr(import_module('jobs.' + class_name), class_name)

        # Create a job object and run
        job = cls(conf, args=sys.argv[2:], user_name=self.user_name)
        job.run()


    def integratePipeDrive(self):
        print("Integrate PipeDrive")
        pipeDriveJobs = ["LOAD_DW_DimCurrentProvider", "LOAD_DW_DimHowIntroduced",
                         "LOAD_DW_DimUser", "LOAD_DW_DimStage",
                         "LOAD_DW_DimPeople", "LOAD_DW_DimOrganization",
                         "LOAD_DW_FactSales"]

        for job in pipeDriveJobs:
            os.system("python job.py {}".format(job))


    def integrateCypherWorx(self):
        print("Integrate CypherWorx")


    def integrateQuickBooks(self):
        print("Integrate QuickBooks")


    def integrateHR(self):
        print("Integrate HR")


    def integrateActiveNet(self):
        print("Integrate ActiveNet")


RightAtSchoolScheduler(True)
# RightAtSchoolScheduler(False)
