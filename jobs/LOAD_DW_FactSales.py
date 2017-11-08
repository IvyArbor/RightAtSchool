from base.jobs import JSONJob, JSONCypherWorxJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer_controller dimension
class FactSales(JSONJob):
    def configure(self):
        self.url = 'https://companydomain.pipedrive.com/v1/deals/?api_token=5119919dca43c62ca026750611806c707f78a745'
        self.auth_user = 'Right At School'
        self.auth_password = 'https://companydomain.pipedrive.com/v1/persons/?api_token=5119919dca43c62ca026750611806c707f78a745'
        #self.data = 'deals'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'FactSales'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "data"
        #self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
            'id',
            'creator_user_id',
            'user_id',
            'person_id',
            'org_id',
            'stage_id',
            'title',
            'value',
            'currency',
            'add_time',
            'update_time',
            'stage_change_time',
            'active',
            'deleted',
            'status',
            'probability',
            'next_activity_date',
            'next_activity_time',
            'next_activity_id',
            'last_activity_id',
            'last_activity_date',
            'lost_reason',
            'visible_to',
            'close_time',
            'pipeline_id',
            'won_time',
            'first_won_time',
            'lost_time',
            'products_count',
            'files_count',
            'notes_count',
            'followers_count',
            'email_messages_count',
            'activities_count',
            'done_activities_count',
            'undone_activities_count',
            'reference_activities_count',
            'participants_count',
            'expected_close_date',
            'last_incoming_mail_time',
            'last_outgoing_mail_time',
            'stage_order_nr',
            'person_name',
            'org_name',
            'next_activity_subject',
            'next_activity_type',
            'next_activity_duration',
            'next_activity_note',
            'formatted_value',
            'rotten_time',
            'weighted_value',
            'formatted_weighted_value',
            'owner_name',
            'cc_email',
            'org_hidden',
            'person_hidden'
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
            'owner_name',
            'value',
            'weighted_value',
            'currency',
            'org_name',
            'person_name',
            'stage_id',
            'status',
            'add_time',
            'update_time',
            'stage_change_time',
            'next_activity_date',
            'last_activity_date',
            'won_time',
            'lost_time',
            'close_time',
            'lost_reason',
            'expected_close_date',
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
            'SalesId',
            'Title',
            'Owner',
            'Value',
            'WeightedValue',
            'Currency',
            'Organization',
            'ContactPerson',
            'StageId',
            'Status',
            'DealCreated',
            'UpdateTime',
            'LastStageChange',
            'NextActivityDate',
            'LastActivityDate',
            'WonTime',
            'LostTime',
            'DealClosedOn',
            'LostReason',
            'ExpectedCloseDate'
        ]

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
