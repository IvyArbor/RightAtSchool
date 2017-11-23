from base.jobs import JSONQuickBooks
from helpers.time import getTimeId

class LOAD_DW_DimClass(JSONQuickBooks):
    def configure(self):
        self.url = 'https://quickbooks.api.intuit.com/v3/company/492414475/query'

        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimClass'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "Class"

        self.headers = {
            'Accept': 'application/json',
            #Read this Key from Configuration
            'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..qXibtHwgHdhNVC195iiEzw.ilx3UzDlqCIbqw2Hny3HNN6u_wkwG9R6jr8ZH6lrKgGN_mAmto13bno0KIweEsSURAQywpcrT8UefrLElYEmQYkmmAqjpLEwmFAT8LI-GofAlp135xWtY7daqHrg1VxXA-Nj5n40k3uJwZy3CWNn9sfIQUkFnN_kdx9p2vGq9Me7NpL1I_zfJJo0WfdpazjeLJ3hF2ZgQ7VUFXHiOiFcKXP-T09tEmW27D58eV_xgZMj_DCySIhzZLx6IUNczTREt2Qgfq75Fvknz6CIorBloutI2LfYQS5Qh4mFWLfqLZGbhP0wa8ka6WKxslD4220X-LxPSEwg62O1PBfTfTTdIMq-rqaDRw7Tb5ydSSZoZSZRnmGZar7dSTwaJ2VwyEOyrKwi4lLHAkXccRIX1Jc0ZsLOp95S8yfzKNSFMxrAkydMCdehILJxxIsAKljFXPifv-tb09Ltqt3zaTaHw1GA7QMW4xgLsw22LUrx1i6i0skRwP869BBQqv5fHH9nonXmfEH9H-m3AgTrleqOrmWrZ7_RfqE7LY6zcN68TN_bxl_YS3Ugc0yvm6k96VvgON0nFTZzu49KTBn6AEG0NklWW2eYQD6F9gPCRO1k9gZHHY8PLnuDKrEp9119_nPlvUsVs0CiJKiKVanQI38JPQJh71KFQ-Tog2YKHwZNhFZgoUJtbELevE3rJEPfBZJLmyyOwgHNONh5GVrmoCA_jW3iWNat9_0qd2Lvcc9fXv9aOLqMJt1Kpe6MNSmpsgO8n4w2E0YMoP8UxEOgAE3MN52gWIglwnFHlgqSHyQbUQSU27z8RC2Kwj6gUjtcF08rXIN1._fKjr6uOWAlQkUzzPIFblw",
            'cache-control': "no-cache",
            #'User-Agent': 'python-quickbooks V3 library',
        }

        self.querystring = {"query": "SELECT * FROM Class", "minorversion": "4"}

    def getColumnMapping(self):
        return [
            'Id',
            'SyncToken',
            'Name',
            'SubClass',
            'ParentRef',
            'FullyQualifiedName',
            'Active',
            'domain',
            'sparse',
            'MetaData'
        ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        myfields = [
            'Id',
            'SyncToken',
            'Name',
            'SubClass',
            'ParentRef',
            'FullyQualifiedName',
            'Active',
            'domain',
            'sparse',
            'MetaData'
            ]

        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        databasefieldvalues = [
            'ClassId',
            'SyncToken',
            'Name',
            'SubClass',
            'ParentRef',
            'FullyQualifiedName',
            'Active',
            'Domain',
            'Sparse',
            'CreateTime',
            'LastUpdatedTime'
        ]

        print ("ROW:")
        print (row)

        row["ParentRef"] = row["ParentRef"]["value"] if row["ParentRef"] != None else None
        row["CreateTime"] = row["MetaData"]["CreateTime"] if row["MetaData"] != None else None
        row["LastUpdatedTime"] = row["MetaData"]["LastUpdatedTime"] if row["MetaData"] != None else None

        del row["MetaData"]

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        print("Name Placeholders: ",name_placeholders)
        value_placeholders = ", ".join(['%s'] * len(row))
        print("Value Placeholders: ",value_placeholders)

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
