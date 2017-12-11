from base.jobs import JSONJob
import pymysql
from settings import conf

class LOAD_DW_DimPeople(JSONJob):
    def configure(self):
        self.auth_user = 'Right At School'
        self.auth_password = '5119919dca43c62ca026750611806c707f78a745'
        #self.data = 'people'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimPeople'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "data"

        self.new_id = self.getLastId()
        self.url = 'https://api.pipedrive.com/v1/persons?start='+ str(self.new_id) + '&api_token=5119919dca43c62ca026750611806c707f78a745&limit=500'

    def getColumnMapping(self):
        return [
            'id',
            'company_id',
            'owner_id',
            'org_id',
            'name',
            'first_name',
            'last_name',
            'open_deals_count',
            'related_open_deals_count',
            'closed_deals_count',
            'related_closed_deals_coun',
            'participant_open_deals_count',
            'participant_closed_deals_count',
            'email_messages_count',
            'activities_count',
            'done_activities_count',
            'undone_activities_count',
            'reference_activities_count',
            'files_count',
            'notes_count',
            'followers_count',
            'won_deals_count',
            'related_won_deals_count',
            'lost_deals_count',
            'related_lost_deals_count',
            'active_flag',
            'phone',
            'email',
            'first_cha',
            'update_time',
            'add_time',
            'visible_to',
            'picture_id',
            'next_activity_date',
            'next_activity_time',
            'next_activity_id',
            'last_activity_id',
            'last_activity_dat',
            'timeline_last_activity_time',
            'timeline_last_activity_time_by_owner',
            '1cb167aaf7f9439b006b550192a04e869c43dade',
            'im',
            'postal_address',
            'postal_address_subpremise',
            'postal_address_street_number',
            'postal_address_route',
            'postal_address_sublocality',
            'postal_address_locality',
            'postal_address_admin_area_level_1',
            'postal_address_admin_area_level_2',
            'postal_address_country',
            'postal_address_postal_code',
            'postal_address_formatted_address',
            'last_incoming_mail_time',
            'last_outgoing_mail_time',
            'org_name',
            'owner_name',
            'cc_email',
        ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'id',
            'name',
            'phone',
            'email',
            'add_time',
            'update_time',
            'org_name',
            'owner_name',
            'open_deals_count',
            'next_activity_date',
            'last_activity_dat',
            'won_deals_count',
            'lost_deals_count',
            'closed_deals_count',
            '1cb167aaf7f9439b006b550192a04e869c43dade',
            'last_incoming_mail_time',
            'last_outgoing_mail_time',
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
        databasefieldvalues = [
            'PersonId',
            'Name',
            'Phone',
            'Email',
            'PersonCreated',
            'UpdateTime',
            'Organization',
            'Owner',
            'OpenDeals',
            'NextActivityDate',
            'LastActivityDate',
            'WonDeals',
            'LostDeals',
            'ClosedDeals',
            'Title',
            'LastEmailReceived',
            'LastEmailSent',
        ]

        row["phone"] = row["phone"][0]["value"]
        row["email"] = row["email"][0]["value"]
        #row["title"] = row["1cb167aaf7f9439b006b550192a04e869c43dade"]

        print ("ROW: ")
        print (row)

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()
        #next call start from next id
        #newid = lastid + 1

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

    def getLastId(self):
        cnn = pymysql.connect(user=conf["mysql"]["DW"]["user"], password=conf["mysql"]["DW"]["password"],
                              host=conf["mysql"]["DW"]["host"],
                              database=conf["mysql"]["DW"]["database"])
        cursor = cnn.cursor()

        query = ("SELECT * FROM {}".format(self.target_table))

        lastid = cursor.execute(query)
        print("LAST IDDDDDDDDDDD")
        print(lastid)
        # start with id =0
        newid = lastid
        cursor.close()
        cnn.close()
        return newid