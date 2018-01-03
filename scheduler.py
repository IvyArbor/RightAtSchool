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

        self.scheduler.add_job(self.integratePipeDrive, 'cron', day="1-31", hour=8)
        self.scheduler.add_job(self.integrateCypherWorx, 'cron', day="1-31", hour=9)
        self.scheduler.add_job(self.integrateNovatimeDaily, 'cron', day="1-31", hour=7) #daily report
        self.scheduler.add_job(self.integrateNovatimeDailyPayPeriodDaily, 'cron', day="1-31", hour=19) #pay period daily report
        self.scheduler.add_job(self.integrateNovatimePayPeriod, 'cron', day_of_week=2, hour=20) # weekly pay period
        self.scheduler.add_job(self.integrateQuickBooks, 'cron', day="1-31", hour=9)
        self.scheduler.add_job(self.integrateATS, 'cron', day="1-31", hour=15)
        self.scheduler.add_job(self.integrateHR, 'cron', day="1-31", hour=10)

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
        CyperworxJobs = ["LOAD_DW_DimCourse","LOAD_DW_DimUser_Cypherworx","LOAD_DW_FactRecord"]

        for job in CyperworxJobs:
            os.system("python job.py {}".format(job))

    # Novatime
    def integrateNovatimeDaily(self):
        print("Integrate NovatimeDaily")
        # NovatimeDailyJobs = ["LOAD_DW_FactLabor"]
        os.system("python gmailAutomate.py")
        os.system("python iterateLaborDaily.py")


    def integrateNovatimePayPeriod(self):
        print("Integrate NovatimePayPeriod")
        # NovatimePayPeriodJobs = ["LOAD_DW_FactLabor_PayPeriod"]
        os.system("python gmailAutomate.py")
        os.system("python iterateLaborPayPeriod.py")

    def integrateNovatimeDailyPayPeriodDaily(self):
        print("Integrate NovatimeDaily")
        # NovatimeDailyJobs = ["LOAD_DW_FactLabor"]
        os.system("python gmailAutomate.py")
        os.system("python iterateLaborDaily.py")

    def integrateQuickBooks(self):
        print("Integrate QuickBooks")


    def integrateHR(self):
        print("Integrate HR")

        os.system("python job.py LOAD_DW_DimEmployee")


    def integrateATS(self):
        print("Integrate ATS")
        ATSJobs = ["LOAD_DW_DimApplicant"]

        for job in ATSJobs:
            os.system("python job.py {}".format(job))



RightAtSchoolScheduler(True)
