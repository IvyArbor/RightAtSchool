from base.jobs import CSVJob, SFTCSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from dateutil import parser
from datetime import datetime, timedelta


# class for employee dimension
class LOAD_DW_DimEmployee(SFTCSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimEmployee'
        self.delimiter = ","
        self.quotechar = '"'
        self.ignore_firstline = True
        self.source_table = ''
        self.source_database = ''
        self.sftp = "HR"
        self.file_path = self._getFilePath()
        #self.file_name = 'sources/RightAtSchool_11292017.csv'

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
            'HireDate',
            'NCESId'
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

        row['Team'] = ""
        row['Role'] = ""


        if self._checkRow(cursor, row) == None:
            self.insertDict(cursor, row, self.target_table)
        else:
            self.updateDict(cursor, row, self.target_table)

        self.target_connection.commit()

    def insertDict(self, cursor, row, table_name):
        print("ROW:",row)
        print("TableName:",table_name)
        name_placeholders = ", ".join(["`{}`".format(s) for s in row.keys()])
        value_placeholders = ", ".join(['%s'] * len(row))

        # insert values
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))

    def updateDict(self, cursor, row, table_name):
        exclude_keys = []
        for key in row:
            if row[key] == "" or row[key] == None:
                exclude_keys.append(key)

        for key in exclude_keys:
            del row[key]

        sql = 'UPDATE {} SET {} WHERE `EmployeeId`={}'.format(table_name, ', '.join('{}=%s'.format(k) for k in row), row["EmployeeId"])
        cursor.execute(sql, tuple(row.values()))

    def _checkRow(self, cursor, row):
        query = """
            SELECT `EmployeeId`
            FROM `DimEmployee`
            WHERE `EmployeeId` = %s
            LIMIT 1
        """

        cursor.execute(query, (row["EmployeeId"]))

        query_result = cursor.fetchone()
        if query_result == None:
            return None
        else:
            return 1

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

    def _getFilePath(self):
        d = datetime.now() - timedelta(days=1)
        return "/mnt/sftp-filetransfer-bucket/RightAtSchool_{}{}{}.csv".format(d.month, d.day, d.year)
