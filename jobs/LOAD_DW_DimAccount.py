from base.jobs import JSONQuickBooks
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId

class LOAD_DW_DimAccount(JSONQuickBooks):
    def configure(self):
            self.url = 'https://sandbox-quickbooks.api.intuit.com/v3/company/193514649567984/query'
            self.target_database = 'rightatschool_testdb'
            self.target_table = 'DimAccount'
            self.source_table = ''
            self.source_database = ''
            self.object_key = 'Account'
            self.headers = {
                'Accept': 'application/json',
                #Read this Key from Configuration
                'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..nuaYCRN4pL8OI_pS0E8Uiw.fQVIHFnGvWMspVJ9gxcFbvE2IoDZ5Ov7thB4C9yQZiLBLVyBqr2fw9dvThAR16bkrSFNWphdCIVGmzT80TM4Eh94ogQfhz3uFs3E7mWlS2OyP2skvJVzkT6GfS7sL0mSaWS2bC5z6JobJAxIwV3tcbPQQo3ZlX1VEN6uCLgoyKJOzec0NeDDQPe16k_Zt_ezIe3HQKX6MpqbExjUEOjeLNNtjlcFPalcXntEBMdz0yT0RAzMMdC7b6ZFpUhNIxmiyXNkzuDENjwgMlxr9_jjB__Lfm0rrYxhYd3ypdhbSkF3FZzNbv7bnnQ_C9B0pQWJWt04fQynfNwvmERJ2yDx77i7aJuKDSXAx7lsITYAOSFCRUCFQ-YYhpfL3MFU5h8xD2fCd6yfuo3_b0lbVE4_mAvQJ0BVJDKZjELdgxbZD8I4K5dv-p503pL8aRYHTn9YslE_7A5jeSwytA467XnQ7Zy4DbzNG2sFHAr1upvdivE4aT20BlC-bnbRF8WFAyjKN6_MMDPVp5E3041KaSQNLNKlBdQ7vDLc6HoUAV2iIvFPaMwj_boHAe5cSukfof5sz2z4v4rd51LoCLGsroiBdkO3MqR6yzh1nOMwfPNPE4fbyLeXL_pKW9MFZ6cQuJkrS8hM44fmfxQOh-R53roYP1pTokz2Wo9mViNARD_G2IpXSENICoeVtOxxLFjJtmVA_e4TzNs3gFPO_uF-U1SUychJxbzBFLiDis86YlzjGLGBNsQHIH1gxUfIY-OC1JZ46advLFeGNTKzvlOT2A9ykuD7XGg1T-lBZSzY_8c0FQcjJTsRsEWbp96EXCAnXScaW9Q7wVWVybKeeV0VywZ1Ug.i-PRvkGrvwLrJLtKWJGmmQ",
                'cache-control': "no-cache",
                #'User-Agent': 'python-quickbooks V3 library',
            }

            self.querystring = {"query": "SELECT * FROM Account STARTPOSITION 1 MAXRESULTS 400", "minorversion": "4"}
            #SELECT * FROM Account STARTPOSITION 1 MAXRESULTS 400

    def getColumnMapping(self):
        return [
            'Id',
            'Name',
            'domain',
            'sparse',
            'SyncToken',
            'SubAccount',
            'ParentRef',
            'Description',
            'FullyQualifiedName',
            'Active',
            'Classification',
            'AccountType',
            'AccountSubType',
            'AcctNum',
            'CurrentBalance',
            'CurrentBalanceWithSubAccounts',
            'CurrencyRef',
            'TaxCodeRef',
            'AccountAlias',
            'TxnLocationType',
            'MetaData'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Id',
            'Name',
            'domain',
            'sparse',
            'SyncToken',
            'SubAccount',
            'ParentRef',
            'Description',
            'FullyQualifiedName',
            'Active',
            'Classification',
            'AccountType',
            'AccountSubType',
            'AcctNum',
            'CurrentBalance',
            'CurrentBalanceWithSubAccounts',
            'CurrencyRef',
            'TaxCodeRef',
            'AccountAlias',
            'TxnLocationType',
            'MetaData'
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
            'AccountId',
            'Name',
            'Domain',
            'Sparse',
            'SyncToken',
            'SubAccount',
            'ParentRef',
            'Description',
            'FullyQualifiedName',
            'Active',
            'Classification',
            'AccountType',
            'AccountSubType',
            'AcctNum',
            'CurrentBalance',
            'CurrentBalanceWithSubAccounts',
            'CurrencyRef',
            'TaxCodeRef',
            'AccountAlias',
            'TxnLocationType',
            'CreateTime',
            'LastUpdatedTime',
        ]
        print("ROW: ")
        print(row)

        row["Id"] = int(row["Id"])
        row["ParentRef"] = row["ParentRef"]['value'] if row["ParentRef"] != None else None
        row["CurrencyRef"] = row["CurrencyRef"]['value'] if row["CurrencyRef"] != None else None
        row["CreateTime"] = row["MetaData"]["CreateTime"] if row["MetaData"] != None else None
        row["LastUpdatedTime"] = row["MetaData"]["LastUpdatedTime"] if row["MetaData"] != None else None
        del row["MetaData"]

        #relate values to DimTime values
        # row["CreateTime"] = getTimeId(cursor, self.target_connection, row["CreateTime"])
        # row["LastUpdatedTime"] = getTimeId(cursor, self.target_connection, row["LastUpdatedTime"])

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        # print('NAME PLACEHOLDERS')
        # print(name_placeholders)
        value_placeholders = ", ".join(['%s'] * len(row))
        # print('VALUE PLACEHOLDERS')
        # print(value_placeholders)

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
