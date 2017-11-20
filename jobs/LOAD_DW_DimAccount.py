from base.jobs import JSONQuickBooks
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId

class LOAD_DW_DimAccount(JSONQuickBooks):
    def configure(self):
        self.url = 'https://quickbooks.api.intuit.com/v3/company/492414475/query?query=select%20%2A%20from%20account'
        self.auth_user = 'Right At School'
        self.auth_password = '5119919dca43c62ca026750611806c707f78a745'
        #self.data = 'people'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimAccount'
        self.source_table = ''

        self.source_database = ''
        self.object_key = "Account"
        #self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
            'Name',
            'SubAccount',
            'ParentRef',
            'Description',
            'FullyQualifiedName',
            'Active',
            'Classification',
            'AccountType',
            'AccountSubType',
            'CurrentBalance',
            'CurrentBalanceWithSubAccounts',
            'AcctNum',
            'CurrencyRef',
            'domain',
            'sparse',
            'Id',
            'SyncToken',
            'MetaData',
            'TaxCodeRef',
            'AccountAlias',
            'TxnLocationType'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'AccountId',
            'Name',
            'Domain',
            'Sparse',
            'SyncToken',
            'CreateTime',
            'LastUpdatedTime',
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
            'CreateTime',
            'LastUpdatedTime',
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
        ]

        row["CurrencyRef"] = row["CurrencyRef"][0]["value"]
        row["CreateTime"] = row["MetaData"][0]["CreateTime"]
        row["LastUpdatedTime"] = row["MetaData"][0]["LastUpdatedTime"]
        del row["CurrencyRef"]
        del row["MetaData"]

        print ("ROW: ")
        print (row)

        #relate values to DimTime values
        row["CreateTime"] = getTimeId(cursor, self.target_connection, row["CreateTime"])
        row["LastUpdatedTime"] = getTimeId(cursor, self.target_connection, row["LastUpdatedTime"])

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

