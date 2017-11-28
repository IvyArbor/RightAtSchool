from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from dateutil import parser
from datetime import datetime


# class for employee dimension
class LOAD_DW_DimEmployee(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimEmployee'
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/RightAtSchool_11212017.csv'

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
            'Employment Status',
            'Hire Date'
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
           'Employment Status',
           'Hire Date'
       ]

        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
           #print('new:', newrow)
        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):

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
                'HireDate',
                'Team',
                'Role'
            ]

            #converts data from '25-10-2017' to '2017-10-25'
        #    row["Effective Date"]= datetime.strptime(row["Effective Date"],'%d-%m-%y').strftime('%Y/%m/%d')
            row["Effective Date"] = parser.parse(row["Effective Date"])
            row["Hire Date"] = parser.parse(row["Hire Date"])
            row['Team']=""
            row['Role']=""

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print('Name Placeholders:',name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))
            print('Values:',value_placeholders)

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
