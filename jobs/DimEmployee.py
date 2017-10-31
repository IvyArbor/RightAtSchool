from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer dimension
class DimEmployee(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimEmployee'
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/Ascentis_HRIS_Sample.csv'

    def getColumnMapping(self):
        return [
            'Employee ID',
            'First Name',
            'Middle Name',
            'Last Name',
            'Effective Date',
            'Location Name',
            'Department Name',
            'Rate Type',
            'Pay Rate',
            'Job Title',
            'Employment Status'
                ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self,row):
        # print('prepare')
        myfields = [
           'Employee ID',
           'First Name',
           'Middle Name',
           'Last Name',
           'Effective Date',
           'Location Name',
           'Department Name',
           'Rate Type',
           'Pay Rate',
           'Job Title',
           'Employment Status'
       ]

        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
         #newrow = { f:row[f] for f in set(myfields) }
        #    print('new:', newrow)
        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        # print('prep:',row)
        # print(row['RecType'])
        # target.insert(row)
        # print("Inserting row:")
        # row.keys()
        if row["Employee ID"] != "Employee ID":
            databasefieldvalues = [
                'EmployeeId',
                'FirstName',
                'MiddleName',
                'LastName',
                'EffectiveDate',
                'LocationName',
                'DepartmentName',
                'RateType',
                'PayRate',
                'JobTitle',
                'EmploymentStatus',
                'Team',
                'Role'
            ]

            #converts data from '25-10-2017' to '2017-10-25'
            #row["Effective Date"]= datetime.strptime(row["Effective Date"],'%d-%m-%y').strftime('%Y/%m/%d')
            row["Effective Date"] = parser.parse(row["Effective Date"])
            row['Team']=""
            row['Role']=""

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
