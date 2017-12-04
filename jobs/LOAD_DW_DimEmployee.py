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
        self.file_name = 'sources/RightAtSchool_11212017.csv'
        self.ignore_firstline = True
        self.source_table = ''
        self.source_database = ''

        # the same as those in the database
    def getColumnMapping(self):
        return [
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
            'HireDate'
            ]

    def getTarget(self):

        # print('target')
      return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    #def prepareRow(self,row):
        # print('prepare')
        #return row

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        #converts the dates in standardized format for MySQL database
        row["EffectiveDate"] = parser.parse(row["EffectiveDate"])
        row["HireDate"] = parser.parse(row["HireDate"])

        row['Team']=""
        row['Role']=""

        #employee = {k:row[k] for k in databasefieldvalues}
        self.insertDict(cursor,row,self.target_table)
        self.target_connection.commit()

    def insertDict(self, cursor, row, table_name):
        print("ROW:",row)
        print("TableName:",table_name)
        name_placeholders = ", ".join(["`{}`".format(s) for s in row.keys()])
        value_placeholders = ", ".join(['%s'] * len(row))

        # insert values
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
