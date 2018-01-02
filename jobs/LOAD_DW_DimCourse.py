from base.jobs import JSONJob, JSONCypherWorxJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension


# class for DimCourse dimension
class LOAD_DW_DimCourse(JSONCypherWorxJob):
    def configure(self):
        self.url = 'https://collabornation.net/lms/api/0.06/course.json'
        self.auth_user = 'Right At School'
        self.auth_password = '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'
        self.data = 'course'
        self.target_database = 'rightatschool_testdb'
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
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
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

        if self._checkRow(cursor, row) == None:
            self.insertDict(cursor, row, self.target_table, databasefieldvalues)
        else:
            self.updateDict(cursor, row, self.target_table, databasefieldvalues)
        self.target_connection.commit()


    def insertDict(self, cursor, row, table_name, databasefieldvalues):
        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        print("Row to be inserted in ",table_name)
        print(row)
        # insert values
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))

    def updateDict(self, cursor, row, table_name, databasefieldvalues):
        print("Row to be updated in ",table_name)
        print(row)
        sql = 'UPDATE {} SET {} WHERE `CourseId`={}'.format(table_name, ', '.join('{}=%s'.format(k) for k in databasefieldvalues), row["id"])
        cursor.execute(sql, tuple(row.values()))

    def _checkRow(self, cursor, row):
        query = """
            SELECT `CourseId`
            FROM `DimCourse`
            WHERE `CourseId` = %s
            LIMIT 1
        """

        cursor.execute(query, (row["id"]))

        query_result = cursor.fetchone()
        if query_result == None:
            return None
        else:
            return 1


    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
