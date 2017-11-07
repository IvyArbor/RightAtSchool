from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer dimension
class LOAD_DW_FactFlexRegistration(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactFlexRegistration'
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/FactFlexRegistration.csv'

    def getColumnMapping(self):
        return [
                'Program Name',
                'Program Type',
                'Catalog Number',
                'Program Status',
                'Program Site',
                'Parent Season',
                'Child Season',
                'Department',
                'Daycare Category',
                'Daycare Other Category',
                'Supervisor',
                'Session Name',
                'Facility',
                'Customer ID',
                'Customer First Name',
                'Customer Last Name',
                'Customer Home Phone',
                'Customer Work Phone',
                'Customer Cell Phone',
                'Customer Addr1',
                'Customer Addr2',
                'Customer City',
                'Customer State',
                'Customer Zipcode',
                'Resident Status',
                'Date of birth',
                'Customer Gender',
                'Session Nbr of Enrolled',
                'Session Nbr of Hours',
                'Session Nbr of Classes',
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            #'Program Name',
            #'Program Type',
            'Catalog Number',
            #'Program Status',
            #'Program Site',
            #'Parent Season',
            #'Daycare Category',
            #'Supervisor',
            #'Session Name',
            'Customer ID',
            #'Customer First Name',
            #'Customer Last Name',
            #'Customer Home Phone',
            #'Customer Work Phone',
            #'Customer Cell Phone',
            #'Customer Addr1',
            #'Customer Addr2',
            #'Customer City',
            #'Customer State',
            #'Customer Zipcode',
            #'Date of birth',
            #'Customer Gender',
            'Session Nbr of Enrolled',
            'Session Nbr of Hours',
            'Session Nbr of Classes',
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
        if row["Catalog Number"] != "Catalog Number":
            databasefieldvalues = [
                'ProgramId',
                #'ProgramLocationId',
                'CustomerId',
                #'CustomerLocationId',
                'NumberOfEnrolled',
                'NumberOfHours',
                'NumberOfClasses'
            ]

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()


