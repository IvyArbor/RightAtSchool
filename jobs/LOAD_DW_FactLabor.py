from base.jobs import ExcelJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId


# class for customer dimension
class LOAD_DW_FactLabor(ExcelJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactLabor'
        self.target_table1 = 'DimDepartment'
        self.first_data_row = 2
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/LABOR-REPORT-JHSU_JHSU_-6228-4107.xlsx'
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
            'DepartmentName',
            'LocationDescription',
            'In',
            'Out',
            'RegHrs',
            'WorkDate1',
            'DailyTotal',
            'CombinedRate',
            'TotalPay',
            'Count',
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        row["LocationId"] = row["LocationId"].lstrip().split(" ")[0]
        row["DepartmentId"] = row["DepartmentId"].split(" ")[0].strip()
        row["EmployeeId"] = row["EmployeeId"].split(" ")[0].strip()

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
           # row['DepartmentCategory'] = None
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
            'LocationDescription',
            'In',
            'Out',
            'RegHrs',
            'WorkDate1',
            'DailyTotal',
            'CombinedRate',
            'TotalPay',
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
