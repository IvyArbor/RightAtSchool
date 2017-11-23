from base.jobs import ExcelJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser

class LOAD_DW_FactFinance(ExcelJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactFinance'
        self.first_data_row = 2
        # Uses Unix filename pattern matching
        # Pattern 	Meaning
        # * 	    matches everything
        # ? 	    matches any single character
        # [seq]     matches any character in seq
        # [!seq]    matches any character not in seq
        #self.pick_file_to_process(folder = 'C:/Users/Nurane Kuqi/PycharmProjects/RightAtSchool/sources/', pattern = 'Quickbooks_FactFinance.xlsx')
        # The following values are already set by this method
        # self.bucket_name
        # self.archive_bucket_name
        # self.bucket_folder
        self.file_name = 'sources/Quickbooks_FactFinance.xlsx'
        self.source_database = ''
        self.source_table = ''
        self.sheet_name = 'QuickbooksFactFinance'


    def getColumnMapping(self):
        return [
            '',
            'Account #',
            'Customer',
            'Vendor',
            'Class',
            'Date',
            'Transaction Type',
            'Num',
            'Name',
            'Split',
            'Memo/Description',
            'Amount',
            'Balance',
            ]


    def getTarget(self):
        # dimension = Dimension(
        # 	name='InvoiceSchedule',
        # 	key='InvoiceSchedulePK',
        # 	attributes=['Schedule' ,'Year', 'StartDate', 'EndDate','BillRunDate','FirstPeriodofMonth']
        # )

        #return dimension
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Account #',
            'Customer',
            'Vendor',
            'Class',
            'Date',
            'Transaction Type',
            'Num',
            'Name',
            'Split',
            'Memo/Description',
            'Amount',
            'Balance',

        ]
        print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }
        #print('new:', newrow)

        return newrow
    def insertRow(self, cursor, row):
        # target.scdensure(row)
        if row["Account #"] != "Account #":
            databasefieldvalues = [
                'AccountId',
                'CustomerId',
                'VendorId',
                'Class',
                'Date',
                'TransactionType',
                'Num',
                'Name',
                'Split',
                'MemoDescription',
                'Amount',
                'Balance',
            ]
            print("Inserting row:")
            row.keys()
            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print(name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))
            print(value_placeholders)

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
