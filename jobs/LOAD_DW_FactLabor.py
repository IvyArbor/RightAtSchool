from base.jobs import XlsJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId
import os


fileList = os.listdir("laborReports/")
file = fileList[0]
print("FILEEEEEE:" , file)


# file = os.path.basename("laborReports/LABOR REPORT-JHSU(JHSU)-6418-4149.xls")
# print("FILE", file)

class LOAD_DW_FactLabor(XlsJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactLabor'
        self.target_table1 = 'DimDepartment'
        self.first_data_row = 2
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        #self.file_name = 'laborReports/LABOR REPORT-JHSU(JHSU)-6418-4149.xls'
        self.file_name = 'laborReports/' + file
        self.sheet_name = 'LABOR REPORT-JHSU'
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
            'Count',
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        del row['Location']
        del row['Department']
        #print(row)
        row["LocationId"] = row["LocationId"].lstrip().split(" ")[0]
        departmentName = row["DepartmentId"]
        row["DepartmentId"] = departmentName.split(" ")[0]
        row["EmployeeId"] = row["EmployeeId"].split(" ")[0]
        row['DepartmentName'] = departmentName.split(" ",1)[1].strip('[]')

        statusvalues = {
            '1': 'Open Status',
            '2': 'Approved Status',
            '3': 'Payroll Status',
        }
        try:
            row['ApprovalStatus'] = statusvalues[row['ApprovalStatus']]
        except KeyError:
            row['DepartmentCategory'] = 'The Approval Status value is blank!'

        decriptionvalues = {
            'ADMINISTRATION': 'Non-Program Time',
            'RC BEFORE SCHOOL': 'Program Time',
            'RC AFTER SCHOOL': 'Program Time',
            'RECESS': 'Recess',
            'Training': 'Program Time',
            'SPECIALTY': 'Program Time',
            'JK WRAP': 'Program Time',
            '5TH DAY': 'Program Time',
            'Licensing': 'Non-Program Time',
            'RAS ELC': 'Program Time',
            'None': 'None',
        }
        try:
            row['DepartmentCategory'] = decriptionvalues[row['DepartmentName']]
        except KeyError:
            row['DepartmentCategory'] = ''
        print('PREPARE', row)
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
            'Count',
        ]
        labor = {k: row[k] for k in laborFields}
        self.insertDict(cursor, labor, self.target_table)
        self.target_connection.commit()


    def insertDict(self, cursor, row, table_name):
        print("ROW",row)
        print(table_name)
        name_placeholders = ", ".join(["`{}`".format(s) for s in row.keys()])
        value_placeholders = ", ".join(['%s'] * len(row))

        # insert only unique values for department, ignore duplicates
        sql1 = "INSERT INTO `{}` ({}) VALUES ({}) ".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql1, tuple(row.values()))

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()



