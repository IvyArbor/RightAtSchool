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
            'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..ZhjWnw63x7LpCeGBEI3fQQ.bqtbAN-GpAgpKefQbf8dtXk3ljBV9QH8l6Y8aN-MY_dm0cbHpma1ItMpvJoiyxkkuCq98pyWd-Nnj1a5eE-5IFQE5bq_rM7yTMV50DQlgUHFsA2PjQBooGYVqlC4qFIByO050H5B5aYnKvihF4yq0LYib0Uvcw2RBw-MF0uWqdPa-FtqoMiU7eZ_pOIScPYFTtNxgw2O11N1KOTMK_sflLhUwB5HWH90yl6Rd7sELAr6QgMEMmy4UIRjM4Xbw1WF90bftPsJn56qL_hze0vQSh02Ka1f1kHslV3wirtGb3mVZJk3KJVqRzk-7_dzppOeG2Ffbc-QwLS84MdgCfjK6JOUTefIVYuuqvA8kNi5W1yKatV0p3xTvMBZtM2eOErz6vG4T2iwPsUiolpWn4vwzGp3b3v6szwjAsQd2UyRmBNSLniGhPwXCuZYl5dNDB2G4mEFOpI-8nNheoAIa4jaZZza4Q5q_tHMqjklKUVHqkXnJbK7uUpPjl890gvwbH5YkCzxBP7YpAqJi3ZtVU3Nl8rcaotqpt2nAZdMMrpC6p_7RSowaRWjHDI8tHUl8WU5Fvp_Poekun-wMnDLwYqkpHARFxX2TIAcZzV5kqcGqNVr_Va8FeKoe4ZzcXtb-K2OdWe2FEEu5rmwaaWF0pZEnK4Y9WW6dVSg4dHzmGdkaWxWVbxKG40jZzlUMqBPEto281CiyUcBH_wYiS_Or6tTjdih2W-8o5bIhJAGxr-ACN4_gSU2PiMBMT3VnFjXDZFa4gJE3DXSBrku_Ulx7mWWQs-b4569WjoIV7xP8hBSLfqfeNUNLfm6KRCOo9aCUxqGVgSdOxXZNABzGunxv2yahQ.dHHpFMZ06WdjznfNKa_mpw",
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
