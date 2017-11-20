from base.jobs import JSONQuickBooks
from helpers.time import getTimeId

class LOAD_DW_DimVendor(JSONQuickBooks):
    def configure(self):
        self.url = 'https://sandbox-quickbooks.api.intuit.com/v3/company/193514649567984/query'

        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimVendor'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "Vendor"

        self.headers = {
            'Accept': 'application/json',
            #Read this Key from Configuration
            'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..WxjPZskglUFSfa8R60X_uA.u8AG1cqrCSQBda9neQKSc3lnkYGrvW9wVKNYFJVd7IF5uFmqgrh_LENfEoLIm2m59WwDSbxc26Yqf5Jb7f4geKQketq1xExwryJa7Agr6pllya8un_rboPDcVort2XioqbsKa_1-DqwHSWH_B72Cnxr0b7lIAq02Qdy2ETQVpB2KY4DiM1rNF_6sZcUVcC0LgLEygwG97m_PRA1KLmA9vPVf-O249KV6xzx0nerBrHkLCZff_ZH60IcJY5bL3v4DMQp4ffVAiJZPvWVFCMcp0E39RryUvId2C52JMvSZ7qP7DaXC1mqb3NLqlz71REy0Fp8UtkrQfW_NPoFPmiS77e3nfCWKforyy-PUDbtQderuUqcj2JHFdCj3uGGr6qoGa0XSjBypIKxBoa29KT1-GSVuiJB3TVKIjsMNBg-No-_D-wcpWxRC0HAHXwJXlKDvQiHbl9ukdn0mN3hlgjDeKGZsxfEV2yW_WqdokcTrOzLtYUfJSwvTGQp37Xm_Uum1GyVasoOd9OvckI0o21t8RIV64sns78b2A5LR6hMQnQKg1jBZKohRb7kp0nIKu8JlsXyxecpd3Xs4B4Tjsujo4SoFHBh8U5UAWsM6IVX5i-IxOLdNaKqnTNDPw9ZThbzDIGQvKUEExg-mD3JpRwrQPnHriw6bvc5HwyzZX4y21ljzZJnioYQxRPTJwIpERXK8-D1gqJKFC2MjCAGzcauDsgWP9wiNU7fHA6u--HMQFhlsIorCAl3c0RtHZ3t0J_OpL2YTI1MRyEu6vEcEZHinzs93OpRBu1nvXd9A-NmPhiyt1fWVA5ZMeUvCk2MRsGvanYucU2CazU1BYhG_LWwSKg.sqYfElj3ffdBkMvsT4TMOQ",
            'cache-control': "no-cache",
            #'User-Agent': 'python-quickbooks V3 library',
        }

        self.querystring = {"query": "SELECT * FROM Vendor", "minorversion": "4"}

    def getColumnMapping(self):
        return [
                'Balance',
                'Vendor1099',
                'CurrencyRef',
                'domain',
                'sparse',
                'Id',
                'SyncToken',
                'MetaData',
                'DisplayName',
                'PrintOnCheckName',
                'Active'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        myfields = [
                'Id',
                'DisplayName',
                'PrintOnCheckName',
                'Balance',
                'Vendor1099',
                'CurrencyRef',
                'domain',
                'sparse',
                'SyncToken',
                'MetaData',
                'Active'
            ]

        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        databasefieldvalues = [
            #TBD
        ]

        print ("ROW:")
        print (row)

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
