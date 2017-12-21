from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId
from settings import conf
# import os
#
# fileList = os.listdir("laborReports/")
# file = fileList[0]


# file = os.path.basename("laborReports/LABOR REPORT-JHSU(JHSU)-6418-4149.xls")
# print("FILE", file)

class LOAD_DW_FactLaborOld(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactLabor'
        self.target_table1 = 'DimDepartment'
        self.delimiter = ","
        self.quotechar = '"'
        self.ignore_firstline = False
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'laborReports/LaborHistorical/LABOR - 2017 - MONTHS 1-7.csv'
        #self.file_name = 'laborReports/' + file

    def getColumnMapping(self):
        return [
            'Location    (G1)',
            'Department  (G2)',
            'EmpSeq',
            'Employee',
            'Work Date',
            'Approval Status',
            'TIME.DCOMP',
            'Date',
            'IN',
            'OUT',
            'Reg Hrs',
            'Work Date1',
            'Daily Total',
            'Combined Rate',
            'Total Pay',
            'NCES ID     ',
            'Location    ',
            'Department  ',
            'COUNT'
            ]

    def getTarget(self):
        # print('target')c
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print(row)
        row["Location    (G1)"] = row["Location    (G1)"].split(" ")[0]
        row["DepartmentId"] = row["Department  (G2)"].split(" ")[0]
        departmentName = row["Department  (G2)"]
        row["Department  (G2)"] = departmentName.split(" ")[0]
        row["DepartmentName"] = departmentName.split(" ",1)[1].strip('[]')
        row["Employee"] = row["Employee"].lstrip().split(" ")[0]

        statusvalues = {
            '1': 'Open Status',
            '2': 'Approved Status',
            '3': 'Payroll Status',
        }
        try:
            row['Approval Status'] = statusvalues[row['Approval Status']]
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
        #print('PREPARE', row)
        return row

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        if row["Department  (G2)"] != "Department  (G2)":
            row["Date"] = getTimeId(cursor, self.target_connection, row["Date"])
            row["Work Date"] = getTimeId(cursor, self.target_connection, row["Work Date"])
            row["Work Date1"] = getTimeId(cursor, self.target_connection, row["Work Date1"])

            departmentFields = [
                'DepartmentId',
                'DepartmentName',
                'DepartmentCategory'
            ]
            department = {k: row[k] for k in departmentFields}

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
                'COUNT',
            ]

            del row["DepartmentId"]
            del row["DepartmentName"]
            del row["DepartmentCategory"]
            del row['Location    ']
            del row['Department  ']

            #print(row)
            name_placeholders = ", ".join(["`{}`".format(s) for s in laborFields])
            value_placeholders = ", ".join(['%s'] * len(row))


            # insert only unique values for department, ignore duplicates
            sql1 = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql1, tuple(row.values()))
            #self.insertDict(cursor, labor, self.target_table)
            self.target_connection.commit()


    def insertDict(self, cursor, row, table_name):
        name_placeholders = ", ".join(["`{}`".format(s) for s in row.keys()])
        value_placeholders = ", ".join(['%s'] * len(row))

        # insert only unique values for department, ignore duplicates
        sql1 = "INSERT INTO `{}` ({}) VALUES ({}) ".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql1, tuple(row.values()))

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()






