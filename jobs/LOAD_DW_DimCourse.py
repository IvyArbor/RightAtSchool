from base.jobs import JSONJob, JSONCypherWorxJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension


# class for DimCourse dimension
class LOAD_DW_DimCourse(JSONCypherWorxJob):
    def configure(self):
        self.url = 'https://collabornation.net/lms/api/0.06/course.json'
        self.auth_user = 'Right At School'
        self.auth_password = '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'
        self.data = 'course'
        self.target_database = 'rightatschool_productiondb'
        self.target_table = 'DimCourse'
        self.source_table = ''
        self.source_database = ''

    def getColumnMapping(self):
        return [
                'id',
                'title',
                'score',
                'expiration'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'id',
            'title',
            'score',
            'expiration'
        ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        #print('new:', newrow)

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):

        databasefieldvalues = [
            'CourseId',
            'Title',
            'Score',
            'Expiration'
        ]

        row["title"] = row["title"].encode("utf-8")

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        print("Course Fields: ",name_placeholders)
        value_placeholders = ", ".join(['%s'] * len(row))
        print("Course Values: ",value_placeholders)

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
