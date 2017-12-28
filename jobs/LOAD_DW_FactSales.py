from base.jobs import JSONJob
from helpers.time import getTimeId
import pymysql
from settings import conf

class LOAD_DW_FactSales(JSONJob):
    def configure(self):
        self.auth_user = 'Right At School'
        self.auth_password = 'https://companydomain.pipedrive.com/v1/persons/?api_token=5119919dca43c62ca026750611806c707f78a745'
        #self.data = 'deals'
        self.target_database = 'rightatschool_productiondb'
        self.target_table = 'FactSales'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "data"

        self.new_id = self.getLastId()
        self.url = 'https://companydomain.pipedrive.com/v1/deals/?api_token=5119919dca43c62ca026750611806c707f78a745&start={}&limit=500'.format(self.new_id)

    def getColumnMapping(self):
        return [
            'id',
            #'public_id',
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
        myfields = [
            'id',
            'title',
            'creator_user_id',
            #'owner_name',
            'value',
            'weighted_value',
            'currency',
            'org_id',
            #'org_name',
            'person_id',
            #'person_name',
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
            'expected_close_date'
        ]

        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        databasefieldvalues = [
            'SalesId',
            'Title',
            'UserId',
            'Value',
            'WeightedValue',
            'Currency',
            'OrganizationId',
            'PersonId',
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

        print (row)

        row["add_time"] = getTimeId(cursor, self.target_connection, row["add_time"])
        row["update_time"] = getTimeId(cursor, self.target_connection, row["update_time"])
        row["stage_change_time"] = getTimeId(cursor, self.target_connection, row["stage_change_time"])
        row["next_activity_date"] = getTimeId(cursor, self.target_connection, row["next_activity_date"])
        row["last_activity_date"] = getTimeId(cursor, self.target_connection, row["last_activity_date"])
        row["won_time"] = getTimeId(cursor, self.target_connection, row["won_time"])
        row["lost_time"] = getTimeId(cursor, self.target_connection, row["lost_time"])
        row["close_time"] = getTimeId(cursor, self.target_connection, row["close_time"])
        row["expected_close_date"] = getTimeId(cursor, self.target_connection, row["expected_close_date"])

        row["creator_user_id"] = row["creator_user_id"]["id"]
        row["org_id"] = row["org_id"]["value"]
        row["person_id"] = row["person_id"]["value"]

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

    def getLastId(self):
        cnn = pymysql.connect(user=conf["mysql"]["DW"]["user"], password=conf["mysql"]["DW"]["password"],
                              host=conf["mysql"]["DW"]["host"],
                              database=conf["mysql"]["DW"]["database"])
        cursor = cnn.cursor()

        query = ("SELECT SalesId FROM {} ORDER BY SalesId DESC LIMIT 1".format(self.target_table))
        cursor.execute(query)
        last_id = cursor.fetchone()
        if last_id == None:
            newid = 0
        else:
            newid = last_id[0]

        print("LastID:", newid)
        cursor.close()
        cnn.close()
        return newid
