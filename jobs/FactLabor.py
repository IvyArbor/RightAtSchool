from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser

# class for customer dimension
class FactLabor(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactLabor'
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/NovaTimeDetailReportLC.csv'

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
            'Paycode',
            'IN',
            'In Ex',
            'OUT',
            'Out Ex',
            'Reason',
            'Division    ',
            'Sh/Pay Ex',
            'Reg Hrs',
            'OT-1',
            'OT-2',
            'Daily Total',
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
            'Paycode',
            'IN',
            'In Ex',
            'OUT',
            'Out Ex',
            'Reason',
            'Division    ',
            'Sh/Pay Ex',
            'Reg Hrs',
            'OT-1',
            'OT-2',
            'Daily Total',
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
        # print('prep:',row)
        # print(row['RecType'])
        # target.insert(row)
        # print("Inserting row:")
        # row.keys()
        if row["Location    (G1)"] != "Location    (G1)":
            databasefieldvalues = [
                    'LocationId',
                    'DepartmentId',
                    'EmpSeq',
                    'Employee',
                    'WorkDate',
                    'ApprovalStatus',
                    'TimedComp',
                    'Date',
                    'Paycode',
                    'In',
                    'InEx',
                    'Out',
                    'OutEx',
                    'Reason',
                    'Division',
                    'ShPayEx',
                    'RegHrs',
                    'OT1',
                    'OT2',
                    'DailyTotal',
                    'TotalPay',
                    'Count',
            ]
            #converts data from '10/25/2017' to '2017/10/25'
            #row["Work Date"]= datetime.strptime(row["Work Date"],'%m/%d/%Y').strftime('%Y/%m/%d')
            row["Work Date"] = parser.parse(row["Work Date"])
            
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
