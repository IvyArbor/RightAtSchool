from base.jobs import JSONJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from helpers.time import getTimeId
import pymysql

# connection to database RAS; takes the ID of the last inserted row which later is used for pagination
cnn = pymysql.connect(user='rastestmaster', password='RasTest0',
                              host='rightatschool-testenv.cblobk4u47xy.us-east-2.rds.amazonaws.com',
                              database='rightatschool_testdb')
cursor = cnn.cursor()

query = ("SELECT * FROM DimOrganization")

lastid = cursor.execute(query)
print("LAST ID:",lastid)
newid = lastid
cursor.close()
cnn.close()

# class for Organization dimension
class LOAD_DW_DimOrganization(JSONJob):

    def configure(self):
        self.url = 'https://api.pipedrive.com/v1/organizations?start='+ str(newid) + '&api_token=5119919dca43c62ca026750611806c707f78a745'
        self.auth_user = 'Right At School'
        self.auth_password = '5119919dca43c62ca026750611806c707f78a745'
        self.object_key = 'data'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimOrganization'
        self.target_table1 = 'DimLocation'
        self.source_table = ''
        self.source_database = ''

        #reads all the fields from Pipedrive API
    def getColumnMapping(self):
        return [
            'id',
            'company_id',
            'owner_id',
            'name',
            'open_deals_count',
            'related_open_deals_count',
            'closed_deals_count',
            'related_closed_deals_count',
            'email_messages_count',
            'people_count',
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
            'category_id',
            'picture_id',
            'country_code',
            'first_char',
            'update_time',
            'add_time',
            'visible_to',
            'next_activity_date',
            'next_activity_time',
            'next_activity_id',
            'last_activity_id',
            'last_activity_date',
            'timeline_last_activity_time',
            'timeline_last_activity_time_by_owner',
            'address',
            'address_subpremise',
            'address_street_number',
            'address_route',
            'address_sublocality',
            'address_locality',
            'address_admin_area_level_1',
            'address_admin_area_level_2',
            'address_country',
            'address_postal_code',
            'address_formatted_address',
            'c8d36c70267b1df922299591d3d08e1d7992fd56',
            '71274bbb5d87c782d9419647bc7b94e0f50eeb38',
            'e77369569ad28775e07f07f64179aa730fe80a89',
            'd9d3c11ed8ab078eb2cf1aaedcfb2f4c638a2c6b',
            'owner_name',
            'cc_email',
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # taking only those fields which I will need from Pipedrive API
        myfields = [
            'id',
            'name',
            'owner_id',
            'people_count',
            'open_deals_count',
            'add_time',
            'update_time',
            'next_activity_date',
            'last_activity_date',
            'won_deals_count',
            'lost_deals_count',
            'closed_deals_count',
            'LocationId',
            'address',
            'address_subpremise',
            'address_street_number',
            'address_route',
            'address_sublocality',
            'address_locality',
            'address_admin_area_level_1',
            'address_admin_area_level_2',
            'address_country',
            'address_postal_code',
            'address_formatted_address',
            'c8d36c70267b1df922299591d3d08e1d7992fd56',
            '71274bbb5d87c782d9419647bc7b94e0f50eeb38',
            'e77369569ad28775e07f07f64179aa730fe80a89',
            'd9d3c11ed8ab078eb2cf1aaedcfb2f4c638a2c6b'
            ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        #print('newrow:',newrow)
        return newrow


    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        #Fields that populate only DimOrganization *important:"Names of the fields should be the same as defined in database"
        databasefieldvalues = [
            'OrganizationId',
            'Name',
            'Owner',
            'People',
            'OpenDeals',
            'OrganizationCreated',
            'UpdateTime',
            'NextActivityDate',
            'LastActivityDate',
            'WonDeals',
            'LostDeals',
            'ClosedDeals',
            'LocationId',
            'NumberOfElementarySchools',
            'FreeAndReducedLunch',
            'HowIntroduced',
            'CurrentProvider'
        ]
        #Taking only the fields that should be pushed to DimLocation
        OrganizationLocationFields = [
            row["address"],
            row["address_subpremise"],
            row["address_street_number"],
            row["address_route"],
            row["address_sublocality"],
            row["address_locality"],
            row["address_admin_area_level_1"],
            row["address_admin_area_level_2"],
            row["address_country"],
            row["address_postal_code"],
            row["address_formatted_address"]
          ]
        #Fields that populate DimLocation
        databasefieldvalues1 = [
            'Street',
            'ApartmentOrSuiteNo',
            'HouseNumber',
            'StreetOrRoadName',
            'District',
            'CityOrTownOrVillage',
            'StateOrCounty',
            'Region',
            'Country',
            'ZipOrPostalCode',
            'FullCombinedAddress'
        ]
        #inserting data in DimLocation
        name_placeholders1 = ", ".join(["`{}`".format(s) for s in databasefieldvalues1])
        print('OrgLocationFields',name_placeholders1)
        value_placeholders1 = ", ".join(['%s'] * len(OrganizationLocationFields))
        print('OrgLocationValues',value_placeholders1)

        sql1 = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table1, name_placeholders1, value_placeholders1)
        cursor.execute(sql1, tuple(OrganizationLocationFields))

        #populating "LocationId"(Foreign key field) in DimOrganization with respective values from DimLocation
        print ("Last ID of Organization Location:")
        print (cursor.lastrowid)
        row["LocationId"] = cursor.lastrowid

        del row["address"],
        del row["address_subpremise"],
        del row["address_street_number"],
        del row["address_route"],
        del row["address_sublocality"],
        del row["address_locality"],
        del row["address_admin_area_level_1"],
        del row["address_admin_area_level_2"],
        del row["address_country"],
        del row["address_postal_code"],
        del row["address_formatted_address"]

        row["owner_id"] = row["owner_id"]["name"]

        # modifying DATETIME fields
        row["add_time"] = getTimeId(cursor, self.target_connection, row["add_time"])
        row["update_time"] = getTimeId(cursor, self.target_connection, row["update_time"])
        row["next_activity_date"] = getTimeId(cursor, self.target_connection, row["next_activity_date"])
        row["last_activity_date"] = getTimeId(cursor, self.target_connection, row["last_activity_date"])

        # inserting data in DimOrganization
        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        print('DimOrganizationFields',name_placeholders)
        value_placeholders = ", ".join(['%s'] * len(row))
        print('DimOrganization Values',value_placeholders)

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

        # increases pagination for the next call
        newid = lastid + 1

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
