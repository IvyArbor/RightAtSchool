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
                'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..-Dt8E64kx1clGkRi5yBN3g.VTGKUVmenr55vhWbPsu-reKTxwrHfySNocbH5S_Kc9rc9VnaskA9qygfU1yW6pf7d-rWimUqC81DDCQ26s_7UEFd8m4j0R7W-mMNcufmH7HcNIO-AuhbKBNcR6r0_v6et3BBRaY7pAw0BcCVCkTtnfB9qkSXROLYIqqUCIOyrH0KpOD3d5O9upCXlfFjqwqTCzy_JnQftyRf2ICBpFDnEB7HquODhmetnFcZKrq21UyiTW7PI4KKW1H7E91ZEovbBYss4NDTOxn_DhQJGB4RavLfidJPu-3KvTW0-UHsW0wGP0ZB6SJBVHF5tqWw0RoR7dqOEIAt1KZvk1CSi9oek18nKC8JWjdakch9a_ZbwUirat6LWCQawpNNGR_WmiJWqLfMDKNS3SxxSa7MUAMk2tnZSr4KhTTl0zhBbYaSiHdP_Gm405at1Xb2Wk7nj62_KbapoOL6vfNi_iHsb100IC2_090ujCxM4QSPhwLbRjeof_4HutSqarH9mTMwppwEyK8roO8P8haBsXWBMG0ZzQ5VeO9glU11W7IUbNkx4enivP11qfsXaCSrjVhYj6ewATcsntSmLOgVRo2H5G7Xvt7Ph-1XvGbLzF_CJ7SBGTU33ALyGX7XHaJ7m0guaQr4-ntER5bn6NZQwuTE6P_vCGU0F7rKaFU0cwK8nePs2pYTxKDN0Gpxj2oDCUxRG5u2YlxbGQbl5ssRjcT5UgpZYqBnIulosbmIFv5ejInlGz9Nw_MlWhTMgR2E-x2dmNE-36xm7m2MnHM6d0rR22WCJ8rilv1nS7NdwsGtGq3F6wa1VQ5XQ82secs1dKS4Vc5Rvdjzpre-Q4WtmqbvNIEf0g.rIrlEFtKhwof87D4RAbvSQ",
                'cache-control': "no-cache",
                #'User-Agent': 'python-quickbooks V3 library',
            }

            self.querystring = {"query": "SELECT * FROM Account", "minorversion": "4"}
            #SELECT * FROM Account STARTPOSITION 1 MAXRESULTS 400

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
            'TxnLocationType',
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
            'MetaData'
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
        print("ROW: ")
        print(row)

        row["CurrencyRef"] = row["CurrencyRef"]["value"] if row["CurrencyRef"] != None else None
        row["CreateTime"] = row["MetaData"]["CreateTime"] if row["MetaData"] != None else None
        row["LastUpdatedTime"] = row["MetaData"]["LastUpdatedTime"] if row["MetaData"] != None else None
        del row["CurrencyRef"]
        del row["MetaData"]

        #relate values to DimTime values
        # row["CreateTime"] = getTimeId(cursor, self.target_connection, row["CreateTime"])
        # row["LastUpdatedTime"] = getTimeId(cursor, self.target_connection, row["LastUpdatedTime"])

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        print('name_placeholders')
        print (name_placeholders)

        value_placeholders = ", ".join(['%s'] * len(row))
        print('value_placeholders')
        print(value_placeholders)

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
