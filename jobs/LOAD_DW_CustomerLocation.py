from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from dateutil import parser
from datetime import datetime

#customer-location

# class for customer dimension
class LOAD_DW_CustomerLocation(CSVJob):

    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimCustomer'
        self.target_table1 = 'DimLocation'
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
            'headofhousehold'
            'lastwaiverdate',
            'mailingaddress1',
            'mailingaddress2',
            'mailingcity',
            'mailingstate',
            'mailingzipcode',
            'FamilyID1',
            'FamilyName1',
            'FamilyRole1',
            'FamilyID2',
            'FamilyName2',
            'FamilyRole2',
            'FamilyID3',
            'FamilyName3',
            'FamilyRole3',
            'FamilyID4',
            'FamilyName4',
            'FamilyRole4',
            'FamilyID5',
            'FamilyName5',
            'FamilyRole5',
            'FamilyID6',
            'FamilyName6',
            'FamilyRole6',
            'FamilyID7',
            'FamilyName7',
            'FamilyRole7',
            'FamilyID8',
            'FamilyName8',
            'FamilyRole8',
            'FamilyID9',
            'FamilyName9',
            'FamilyRole9',
            'FamilyID10',
            'FamilyName10',
            'FamilyRole10',
            'FamilyID11',
            'FamilyName11',
            'FamilyRole11',
            'FamilyID12',
            'FamilyName12',
            'FamilyRole12',
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
            'customer_id',
            'LocationId',
            'firstname',
            'lastname',
            'email',
            'homephone',
            'workphone',
            'cellphone',
            'address1',
            'address2',
            'city',
            'state',
            'zipcode',
            'mailingaddress1',
            'mailingaddress2',
            'mailingcity',
            'mailingstate',
            'mailingzipcode',
            'birthdate',
            'grade_id',
            'site_id',
            'gender', #changed from site_id because duplicate values
            'customertype_id',
            'nomail',
            'nopostalmail',
            'retired',
            'MedicalAlert',
            'GeneralAlert',
            'SpecialHandling',
            'notes',
            'emergencyfname1',
            'emergencylname1',
            'emergencyphone1',
            'emergencyrelation1',
            'emergencyfname2',
            'emergencylname2',
            'emergencyphone2',
            'emergencyrelation2',
            'emergency_other_phone1',
            'emergency_other_phone2',
            'agree_receive_text_message',
            'additional_email',
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
        if row["customer_id"] != "customer_id":
            databasefieldvalues = [
                'CustomerId',
                'LocationId',
                'FirstName',
                'LastName',
                'Email',
                'HomePhone',
                'WorkPhone',
                'CellPhone',
                'MailingAddress1',
                'MailingAddress2',
                'MailingCity',
                'MailingState',
                'MailingZipcode',
                'Birthdate',
                'GradeId',
                'SiteId',
                'Gender',
                'CustomertypeId',
                'NoMail',
                'NoPostalMail',
                'Retired',
                'MedicalAlert',
                'GeneralAlert',
                'SpecialHandling',
                'Notes',
                'EmergencyFName1',
                'EmergencyLName1',
                'EmergencyPhone1',
                'EmergencyRelation1',
                'EmergencyFName2',
                'EmergencyLName2',
                'EmergencyPhone2',
                'EmergencyRelation2',
                'EmergencyOtherPhone1',
                'EmergencyOtherPhone2',
                'AgreeReceiveTextMessage',
                'AdditionalEmail'
            ]
            CustomerLocationFields = [
                row["address1"],
                row["address2"],
                row["city"],
                row["zipcode"],
                "",
                row["state"],
                ""
            ]
            databasefieldvalues1 = [
                'Street',
                'AppartmentNu',
                'City',
                'Zipcode',
                'District',
                'State',
                'Region'
            ]
            name_placeholders1 = ", ".join(["`{}`".format(s) for s in databasefieldvalues1])
            value_placeholders1 = ", ".join(['%s'] * len(CustomerLocationFields))
            sql1 = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table1, name_placeholders1, value_placeholders1)
            cursor.execute(sql1, tuple(CustomerLocationFields))
            print ("LAST ID")
            print (cursor.lastrowid)
            row["LocationId"] = cursor.lastrowid

            del row["address1"]
            del row["address2"]
            del row["city"]
            del row["zipcode"]
            del row["state"]
            row["birthdate"] = parser.parse(row["birthdate"])
            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print(name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))


            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))





            self.target_connection.commit()


    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
