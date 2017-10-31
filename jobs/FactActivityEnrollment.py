from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser

# class for customer dimension
class FactActivityEnrollment(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactActivityEnrollment'
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
            'Number of Attendance'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Activity Number',
            'Customer ID',
            'Amount',
            'Amount Incl Tax',
            'Total Enrolled'
        ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }
        print('new:', newrow)

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
                'ActivityId',
                'CustomerId',
                'Amount',
                'AmountIncTax',
                'TotalEnrolled'
            ]


            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print(name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))
            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders,value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
