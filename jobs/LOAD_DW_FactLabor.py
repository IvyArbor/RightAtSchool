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
            'Location    (G1)',
            'Department  (G2)',
            'EmpSeq',
            'Employee',
            'Work Date',
            'Approval Status',
            'TIME.DCOMP',
            'Date',
            'Department   Description',
            'Location     Description',
            'IN',
            'OUT',
            'Reg Hrs',
            'Work Date # 1',
            'Daily Total',
            'Combined Rate',
            'Total Pay',
            'COUNT'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Location    (G1)',
            'Department  (G2)',
            'EmpSeq',
            'Employee',
            'Work Date',
            'Approval Status',
            'TIME.DCOMP',
            'Date',
            'Department   Description',
            'Location     Description',
            'IN',
            'OUT',
            'Reg Hrs',
            'Work Date # 1',
            'Daily Total',
            'Combined Rate',
            'Total Pay',
            'COUNT'
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
            # split field values
            row["Location    (G1)"] = row["Location    (G1)"].split(" ")[0].strip()
            row["Department  (G2)"] = row["Department  (G2)"].split(" ")[0].strip()
            row["Employee"] = row["Employee"].split(" ")[0].strip()

            databasefieldvalues = [
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

            DepartmentFields = [
                row["Department  (G2)"],
                row["Department   Description"],

            ]
            databasefieldvalues1 = [
                'DepartmentId',
                'DepartmentName'
            ]

            row["Date"] = getTimeId(cursor, self.target_connection, row["Date"])
            row["Work Date"] = getTimeId(cursor, self.target_connection, row["Work Date"])
            row["Work Date # 1"] = getTimeId(cursor, self.target_connection, row["Work Date # 1"])

            #assign new values to Approval Status field based on condition
            if row['Approval Status'] == '1':
                row['Approval Status'] = 'Open Status'
                print('The Approval Status value should be Open Status!')
            elif row['Approval Status'] =='2':
                row['Approval Status'] = 'Approved Status'
                print('The Approval Status value should be Approved Status!')
            elif row['Approval Status'] == '3':
                row['Approval Status'] = 'Payroll Status'
                print('The Approval Status value should be Payroll Status!')
            else:
                row['Approval Status'] = ''
                print('The Approval Status value is blank!')


            name_placeholders1 = ", ".join(["`{}`".format(s) for s in databasefieldvalues1])
            value_placeholders1 = ", ".join(['%s'] * len(DepartmentFields))

            # insert only unique values for department, ignore duplicates
            sql1 = "INSERT IGNORE INTO `{}` ({}) VALUES ({}) ".format(self.target_table1, name_placeholders1, value_placeholders1)
            cursor.execute(sql1, tuple(DepartmentFields))
            del row["Department   Description"]

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print(name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))
            print(value_placeholders)

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders,value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
