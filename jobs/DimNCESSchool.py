from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from dateutil import parser
from datetime import datetime

# class for customer dimension
class DimNCESSchool(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimNCESSchool'
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        self.ignore_firstline = True
        self.file_name = 'sources/School Data.csv'

    def getColumnMapping(self):
        return [
            'School Name',
            'State Name [Public School] Latest available year',
            'State Abbr [Public School] Latest available year',
            'School ID - NCES Assigned [Public School] Latest available year',
            'Agency ID - NCES Assigned [Public School] Latest available year',
            'Location Address 1 [Public School] 2014-15',
            'Location Address 2 [Public School] 2014-15',
            'Location City [Public School] 2014-15',
            'County Number [Public School] 2014-15',
            'County Name [Public School] 2014-15',
            'Location ZIP [Public School] 2014-15',
            'Prekindergarten and Kindergarten Students [Public School] 2014-15',
            'Grades 1-8 Students [Public School] 2014-15',
            'Grades 9-12 Students [Public School] 2014-15',
            'Free and Reduced Lunch Students [Public School] 2014-15'
        ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        myfields = [
            'School ID - NCES Assigned [Public School] Latest available year',
            'NCESDistrictId',
            'School Name',
            'State Name [Public School] Latest available year',
            'State Abbr [Public School] Latest available year',
            #'Agency ID - NCES Assigned [Public School] Latest available year',
            'Location Address 1 [Public School] 2014-15',
            #'Location Address 2 [Public School] 2014-15',
            'Location City [Public School] 2014-15',
            'County Name [Public School] 2014-15',
            'County Number [Public School] 2014-15',
            'Location ZIP [Public School] 2014-15',
            'Prekindergarten and Kindergarten Students [Public School] 2014-15',
            'Grades 1-8 Students [Public School] 2014-15',
            'Grades 9-12 Students [Public School] 2014-15',
            'Free and Reduced Lunch Students [Public School] 2014-15'
        ]
        # print('prepare')
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }
        #print('new:', newrow)

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        if row["School ID - NCES Assigned [Public School] Latest available year"] != "School ID - NCES Assigned [Public School] Latest available year":
            databasefieldvalues = [
                'NCESSchoolId',
                'NCESDistrictId',
                'SchoolName',
                'StateName',
                'StateAbbr',
                'Address',
                'City',
                'County',
                'CountyNumber',
                'ZIP',
                'KindergardenStudents',
                'Grades1-8Students',
                'Grades9-12Students',
                'FreeAndReducedLunchStudents'
            ]

            if row["School ID - NCES Assigned [Public School] Latest available year"][0] == "=":
                row["School ID - NCES Assigned [Public School] Latest available year"] = row["School ID - NCES Assigned [Public School] Latest available year"][2:]
                row["School ID - NCES Assigned [Public School] Latest available year"] = row["School ID - NCES Assigned [Public School] Latest available year"][:-1]

            row["NCESDistrictId"] = row["School ID - NCES Assigned [Public School] Latest available year"][:7]

            if row["County Number [Public School] 2014-15"][0] == "=":
                row["County Number [Public School] 2014-15"] = row["County Number [Public School] 2014-15"][2:]
                row["County Number [Public School] 2014-15"] = row["County Number [Public School] 2014-15"][:-1]

            if row["Location ZIP [Public School] 2014-15"][0] == "=":
                row["Location ZIP [Public School] 2014-15"] = row["Location ZIP [Public School] 2014-15"][2:]
                row["Location ZIP [Public School] 2014-15"] = row["Location ZIP [Public School] 2014-15"][:-1]

            if row['Prekindergarten and Kindergarten Students [Public School] 2014-15'].isdigit() == False:
                row['Prekindergarten and Kindergarten Students [Public School] 2014-15'] = None

            if row['Grades 1-8 Students [Public School] 2014-15'].isdigit() == False:
                row['Grades 1-8 Students [Public School] 2014-15'] = None

            if row['Grades 9-12 Students [Public School] 2014-15'].isdigit() == False:
                row['Grades 9-12 Students [Public School] 2014-15'] = None

            if row['Free and Reduced Lunch Students [Public School] 2014-15'].isdigit() == False:
                row['Free and Reduced Lunch Students [Public School] 2014-15'] = None


            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders,value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
