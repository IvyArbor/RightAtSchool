from base.jobs import JSONJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer_controller dimension
class DimOrganization(JSONJob):
    def configure(self):
        self.url = 'https://api.pipedrive.com/v1/organizations?start=0&api_token=5119919dca43c62ca026750611806c707f78a745'
        self.auth_user = 'Right At School'
        self.auth_password = '5119919dca43c62ca026750611806c707f78a745'
        self.object_key = 'data'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimOrganization'
        self.source_table = ''
        self.source_database = ''
        #self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
                "id",
                "company_id",
                "owner_id",
                "name",
                "open_deals_count",
                "related_open_deals_count",
                "closed_deals_count",
                "related_closed_deals_count",
                "email_messages_count",
                "people_count",
                "activities_count",
                "done_activities_count",
                "undone_activities_count",
                "reference_activities_count",
                "files_count",
                "notes_count",
                "followers_count",
                "won_deals_count",
                "related_won_deals_count",
                "lost_deals_count",
                "related_lost_deals_count",
                "active_flag",
                "category_id",
                "picture_id",
                "country_code",
                "first_char",
                "update_time",
                "add_time",
                "visible_to",
                "next_activity_date",
                "next_activity_time",
                "next_activity_id",
                "last_activity_id",
                "last_activity_date",
                "timeline_last_activity_time",
                "timeline_last_activity_time_by_owner",
                "address",
                "address_subpremise",
                "address_street_number",
                "address_route",
                "address_sublocality",
                "address_locality",
                "address_admin_area_level_1",
                "address_admin_area_level_2",
                "address_country",
                "address_postal_code",
                "address_formatted_address",
                "c8d36c70267b1df922299591d3d08e1d7992fd56",
                "71274bbb5d87c782d9419647bc7b94e0f50eeb38",
                "e77369569ad28775e07f07f64179aa730fe80a89",
                "d9d3c11ed8ab078eb2cf1aaedcfb2f4c638a2c6b",
                "owner_name",
                "cc_email"
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
                "id",
                "name",
                "owner_id",
                "people_count",
                "open_deals_count",
                "add_time",
                "update_time",
                "next_activity_date",
                "last_activity_date",
                "won_deals_count",
                "lost_deals_count",
                "closed_deals_count",
                # "address",
                # "address_subpremise",
                # "address_street_number",
                # "address_route",
                # "address_sublocality",
                # "address_locality",
                # "address_admin_area_level_1",
                # "address_admin_area_level_2",
                # "address_country",
                # "address_postal_code",
                # "address_formatted_address",
                "c8d36c70267b1df922299591d3d08e1d7992fd56",
                "71274bbb5d87c782d9419647bc7b94e0f50eeb38",
                "e77369569ad28775e07f07f64179aa730fe80a89",
                #"d9d3c11ed8ab078eb2cf1aaedcfb2f4c638a2c6b",
                "owner_name"

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
            # 'Address',
            # 'ApartmentOrSuiteNo',
            # 'HouseNumber',
            # 'StreetOrRoadName',
            # 'DistrictOrSublocality',
            # 'CityOrTownOrVillageOrLocality',
            # 'StateOrCounty',
            # 'Region',
            # 'Country',
            # 'ZipCode',
            # 'FullCombinedAddress',
            'NumberOfElementarySchools',
            'FreeAndReducedLunch',
            'HowIntroduced',
            'CurrentProvider'
        ]

        row["owner_id"] = row["owner_id"]["name"]
        print ("ROW: ")
        print (row)


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
