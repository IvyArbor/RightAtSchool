from base.jobs import JSONJob, JSONCypherWorxJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer_controller dimension
class FactRecord(JSONCypherWorxJob):
    def configure(self):
        self.url = 'https://collabornation.net/lms/api/0.06/record.json'
        self.auth_user = 'Right At School'
        self.auth_password = '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'
        self.data = 'record'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactRecord'
        self.source_table = ''
        self.source_database = ''
        #self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
                'record_id',
                'user_id',
                'course_id',
                'course',
                'status',
                'assignment_source',
                'score',
                'finish_date',
                'created_date',
                'expiration_date',
                'time',
                'course_link',
                'link'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
                'record_id',
                'user_id',
                'course_id',
                'course',
                'status',
                'assignment_source',
                'score',
                'finish_date',
                'created_date',
                'expiration_date',
                'time',
                'course_link',
                'link'
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
        databasefieldvalues = [
            'Id',
            'UserId',
            'CourseId',
            'Status',
            'AssignmentSource',
            'Score',
            'FinishDate',
            'CreateDate',
            'ExpirationDate',
            'Time',
            'CourseLink',
            'Link'
        ]

        row["finish_date"] = datetime.fromtimestamp(int(row["finish_date"])) if row["finish_date"] != "0" else print ("Can't get time for finish_date")
        row["created_date"] = datetime.fromtimestamp(int(row["created_date"])) if row["created_date"] != "0" else print ("Can't get time for created_date")
        row["expiration_date"] = datetime.fromtimestamp(int(row["expiration_date"])) if row["expiration_date"] != "0" else print ("Can't get time for expiration_date")
        del row["course"]

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

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
