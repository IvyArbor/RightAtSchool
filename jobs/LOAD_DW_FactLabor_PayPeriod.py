from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId
import os


# file = os.path.basename("laborReports/LABOR REPORT-JHSU(JHSU)-6418-4149.xls")
# print("FILE", file)

fileList = os.listdir("payPeriodReports/")
filename = fileList[0]


class LOAD_DW_FactLabor(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'TestFactLabor'
        self.target_table1 = 'TestDimDepartment'
        self.delimiter = ","
        self.quotechar = '"'
        self.ignore_firstline = False
        self.source_table = ''
        self.source_database = ''
        # self.file_name = 'laborReports/TestCSV.csv'
        self.file_name = 'payPeriodReports/' + filename
    def getColumnMapping(self):
        return [
            'LocationId',
            'DepartmentId',
            'EmployeeSeq',
            'EmployeeId',
            'WorkDate',
            'ApprovalStatus',
            'TimedComp',
            'Date',
            'In',
            'Out',
            'RegHrs',
            'WorkDate1',
            'DailyTotal',
            'CombinedRate',
            'TotalPay',
            'NCESID',
            'Location',
            'Department',
            'COUNT'
            ]

    def getTarget(self):
        # print('target')c
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        if 'COUNT' not in row:
            row['COUNT'] = ''
        del row['Location']
        del row['Department']
        #print(row)
        row["LocationId"] = row["LocationId"].lstrip().split(" ")[0]
        departmentName = row["DepartmentId"]
        row["DepartmentId"] = departmentName.split(" ")[0]
        empid = row["EmployeeId"].lstrip().split(" ")
        row["EmployeeId"] = empid[0]
        row['DepartmentName'] = departmentName.split(" ",1)[1].strip('[]')
        statusvalues = {
            '1': 'Open Status',
            '2': 'Approved Status',
            '3': 'Payroll Status',
        }

        print("EmployeeId::::::")
        print(row['EmployeeId'])
        print("EmployeeId::::::")

        try:
            row['ApprovalStatus'] = statusvalues[row['ApprovalStatus']]
        except KeyError:
            row['DepartmentCategory'] = 'The Approval Status value is blank!'

        descriptionvalues = {
            'ADMINISTRATION': 'Non-Program Time',
            'RC BEFORE SCHOOL': 'Program Time',
            'RC AFTER SCHOOL': 'Program Time',
            'RECESS': 'Recess',
            'Training': 'Training',
            'SPECIALTY': 'Program Time',
            'JK WRAP': 'Program Time',
            '5TH DAY': 'Program Time',
            '5TH DAY FRIDAY': 'Program Time',
            'Licensing': 'Licensing',
            'RAS ELC': 'Program Time',
            'SUMMER CAMP': 'Program Time',
            'TUTOR RIGHT': 'Non-Program Time',
            'EOD': 'Electives',
            'None': 'None',
        }
        try:
            row['DepartmentCategory'] = descriptionvalues[row['DepartmentName']]
        except KeyError:
            row['DepartmentCategory'] = ''
        return row


    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        row["Date"] = getTimeId(cursor, self.target_connection, row["Date"])
        row["WorkDate"] = getTimeId(cursor, self.target_connection, row["WorkDate"])
        row["WorkDate1"] = getTimeId(cursor, self.target_connection, row["WorkDate1"])

        departmentFields = [
            'DepartmentId',
            'DepartmentName',
            'DepartmentCategory',
        ]
        department = {k:row[k] for k in departmentFields}
        cursor.execute('SELECT count(*) FROM {} WHERE DepartmentId = %s'.format(self.target_table1),
                       (department['DepartmentId']))
        if not cursor.fetchone()[0] > 0:
            self.insertDict(cursor, department, self.target_table1)
        self.target_connection.commit()

        laborFields = [
            'LocationId',
            'DepartmentId',
            'EmployeeSeq',
            'EmployeeId',
            'WorkDate',
            'ApprovalStatus',
            'TimedComp',
            'Date',
            'In',
            'Out',
            'RegHrs',
            'WorkDate1',
            'DailyTotal',
            'CombinedRate',
            'TotalPay',
            'NCESID',
            'COUNT'
        ]


        row['Location'] = ''
        row['Department'] = ''
        labor = {k: row[k] for k in laborFields}
        self.insertDict(cursor, labor, self.target_table)
        self.target_connection.commit()

    def insertDict(self, cursor, row, table_name):
        name_placeholders = ", ".join(["`{}`".format(s) for s in row.keys()])
        value_placeholders = ", ".join(['%s'] * len(row))

        # insert only unique values for department, ignore duplicates
        sql1 = "INSERT INTO `{}` ({}) VALUES ({}) ".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql1, tuple(row.values()))

    def close(self):
        """Here we should archive the file instead"""


