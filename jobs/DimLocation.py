from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from dateutil import parser
from datetime import datetime


# IN ORDER TO FUNCTION ADD CUSTOMERID TO FIELD NAMES AND DATABASE FIELDS

# class for customer dimension
class Location(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimLocation'
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/Customers_Extract.csv'

    def getColumnMapping(self):
        return [
            'customer_id',
            'customertype_id',
            'geographic_area_id',
            'lastpayee_id',
            'site_id',
            'grade_id',
            'firstname',
            'lastname',
            'address1',
            'address2',
            'city',
            'state',
            'zipcode',
            'homephone',
            'workphone',
            'cellphone',
            'faxphone',
            'pagerphone',
            'otherphone',
            'email',
            'ssn',
            'birthdate',
            'carddate',
            'entrydate',
            'gender',
            'generalalert',
            'headofhousehold',
            'lastwaiverdate',
            'mailingaddress1',
            'mailingaddress2',
            'mailingcity',
            'mailingstate',
            'mailingzipcode',
            'medicalalert',
            'notes',
            'occupation',
            'resident',
            'specialhandling',
            'lastphotographed',
            'password1',
            'password2',
            'question',
            'answer',
            'emergencyfname1',
            'emergencylname1',
            'emergencyphone1',
            'emergencyrelation1',
            'emergencyfname2',
            'emergencylname2',
            'emergencyphone2',
            'emergencyrelation2',
            'pa_popid',
            'nomail',
            'externalid',
            'datemodified',
            'externalidtext',
            'retired',
            'gradeagedelta',
            'nopostalmail',
            'not_online_activated',
            'login_created',
            'login_used',
            'occupation_id',
            'interest_date',
            'county',
            'membership_overusage',
            'country',
            'mailingcountry',
            'mailing_name',
            'can_be_scheduled',
            'human_resource_id',
            'failed_logon_count',
            'age_category_id',
            'customer_title_id',
            'middlename',
            'residency_expires_date',
            'soundex_firstname',
            'soundex_lastname',
            'emergency_other_phone1',
            'emergency_other_phone2',
            'soundex_middlename',
            'is_temp_password',
            'legal_name',
            'additional_email',
            'late_fee_date',
            'carrier_id',
            'agree_receive_text_message',
            'Created Online / Staff Side',
            'lighting_pin',
            'row_version',
            'person_id',
            'is_enc_password',
            'password_id',
            'Subscription List']

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'address1',
            'address2',
            'city',
            'zipcode',
            'district',
            'state',
            'region'
        ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }
        print('new:', newrow)

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        # print('prep:',row)
        # print(row['RecType'])
        # target.insert(row)
        # print("Inserting row:")
        # row.keys()
        if row["city"] != "city":
            databasefieldvalues = [
                'Street',
                'AppartmentNu',
                'City',
                'Zipcode',
                'District',
                'State',
                'Region'
            ]


            row['district'] = ""
            row['region'] = ""


            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print(name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders,value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
