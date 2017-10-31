from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from dateutil import parser
from datetime import datetime

# class for customer dimension
class DimSchoolLocation(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimSchoolLocation'
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/School.csv'

    def getColumnMapping(self):
        return [
            'STATE',
            'DISTRICT',
            'SCHOOL NAME',
            'NCES SCHOOL ID'
        ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'NCES SCHOOL ID',
            'SCHOOL NAME',
            'DISTRICT',
            'STATE'
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
        if row["NCES SCHOOL ID"] != "NCES SCHOOL ID":
            databasefieldvalues = [
                'NCESSchoolId',
                'SchoolName',
                'District',
                'State'
            ]


            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders,value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
