from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer dimension
class DimActivity(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimActivity'
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
            'Activity Category',
            'Activity Department',
            'Activity External Number',
            'Activity Name',
            'Activity Number',
            'Activity Status',
            'Activity Type',
            'Center',
            'Customer ID',
            'Days of Week',
            'End Date',
            'End Time',
            'Facility',
            'Facility Type',
            'Instructor End Date',
            'Instructor Name',
            'Instructor Role',
            'Instructor Start Date',
            'League Name',
            'Max Age',
            'Maximum Grade',
            'Min Age',
            'Minimum Grade',
            'Organization',
            'Parent Activity',
            'Payer Address 1',
            'Payer Address 2',
            'Payer City',
            'Payer Cell Phone',
            'Payer Email',
            'Payer First Name',
            'Payer Home Phone',
            'Payer ID',
            'Payer Last Name',
            'Payer State',
            'Payer Work Phone',
            'Payer Zipcode',
            'Private Lesson First Date',
            'Private Lesson Last Date',
            'Season',
            'Site',
            'Start Date',
            'Start Time',
            'Sub Category Name',
            'Supervisor',
            'Tax Receipt Eligibility',
            'Team Contact First Name',
            'Team Contact Last Name',
            'Team Name',
            'Term',
            'Transaction Date',
            'Transaction Type',
            'Week of Month',
            'Amount',
            'Amount Incl Tax',
            'Total Enrolled',
            'Nbr of Hours',
            'Nbr of Sessions',
            'Number of Attendance',
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Activity Category',
            'Activity Name',
            'Activity Number',
            'Activity Status',
            'Activity Type',
            'Days of Week',
            'End Date',
            'End Time',
            'Organization',
            'Season',
            'Site',
            'Start Date',
            'Start Time',
            'Transaction Date',
            'Transaction Type',
            'Week of Month',
        ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }

        #print('new:', newrow)

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        # print('prep:',row)
        # print(row['RecType'])
        # target.insert(row)
        # print("Inserting row:")
        # row.keys()
        if row["Activity Number"] != "Activity Number":
            databasefieldvalues = [
                'ActivityCategory',
                'ActivityName',
                'ActivityNumber',
                'ActivityStatus',
                'ActivityType',
                'DaysOfWeek',
                'EndDate',
                'EndTime',
                'Organization',
                'Season',
                'Site',
                'StartDate',
                'StartTime',
                'TransactionDate',
                'TransactionType',
                'WeekOfMonth',
            ]

            row["Activity Number"] = int(row["Activity Number"])
            row["End Date"] = parser.parse(row["End Date"])
            row["Start Date"] = parser.parse(row["Start Date"])
            row["Transaction Date"] = parser.parse(row["Transaction Date"])

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

    def parseTime(self, dt):
        date = parser.parse(dt)

        result = {}
        result["Year"] = date.year
        result["Quarter"] = int(math.ceil(date.month / 3.))
        result["Month"] = date.month
        result["Week"] = date.isocalendar()[1]
        result["Day"] = date.day
        result["DayOfWeek"] = date.weekday() + 1

        return result
