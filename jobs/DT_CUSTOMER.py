from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime

'''
DB Name: rightatschool_testdb
Host: rightatschool-test.c6ac6cyneqii.us-east-1.rds.amazonaws.com
Port: 3306
Master User: ras
Master Pass: RaS1p38!BV44jw
'''
class DT_CUSTOMER(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'customer_dimension'
        self.delimiter = ","
        self.quotechar = '"'
        #self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'C:/Users/Nurane/PycharmProjects/rightatschool/sources/Customers_Extract.csv'

    def getColumnMapping(self):
        return [
            'customer_id',
            'customertype_id',
            'geographic_area_id',
            'lastpayee_id',
            'site_id',
            'grade_id',
            'firstname',
            'lastname,'
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
        # RebateRejectReason = Dimension(
        #     name='RebateRejectReason',
        #     key='RebateRejectPK',
        #     attributes = ['RecType','SubNbr20','PatSexCd'],
        #     #ALL columns except the Natural Keys (lookupatts) are Type1 so no need to list them
        #     #PygramETL will include ALL of the non-lookupatts as type1atts
        #     #type1atts=[]
        # )
        # return RebateRejectReason
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    # def prepareRow(self, row):
    #     self.updateAuditTracking(row)
    #     return row

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        # print(row['RecType'])
        #target.insert(row)
        # print("Inserting row:")
        # row.keys()
        try:
            del row[
                'geographic_area_id',
                'lastpayee_id',
                'faxphone',
                'pagerphone',
                'otherphone',
                'ssn',
                'carddate',
                'entrydate',
                'gender',
                'headofhousehold',
                'lastwaiverdate',
                'occupation',
                'resident',
                'lastphotographed',
                'password1',
                'password2',
                'question',
                'answer',
                'pa_popid',
                'externalid',
                'datemodified',
                'externalidtext',
                'gradeagedelta',
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
                'emergency_other_phone2',
                'soundex_middlename',
                'is_temp_password',
                'legal_name',
                'late_fee_date',
                'carrier_id',
                'Created',
                'Online / Staff',
                'Side',
                'lighting_pin',
                'row_version',
                'person_id',
                'is_enc_password',
                'password_id',
                'Subscription List'
            ]
            #row['RejectYearMonth'] = datetime.today().strftime('%Y%m')
            #row['PatBirthDt'] = row['PatBirthDt'][:8]
            #row['SubmitDate'] = row['SubmitDate'][:8]
            #row['PaidCendt'] = row['PaidCendt'][:8]
            #row['FillCentdt'] = row['FillCentdt'][:8]

            name_placeholders = ", ".join(["`{}`".format(s) for s in row.keys()])
            value_placeholders = ", ".join(['%s'] * len(row))
            sql = "INSERT INTO `{}` ({}) VALUES ({})".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            # print(tuple(row.values()))
            # print(sql)
        except Exception as e:
            print(e)
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
