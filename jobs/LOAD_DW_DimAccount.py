from base.jobs import JSONQuickBooks
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
from helpers.time import getTimeId

class LOAD_DW_DimAccount(JSONQuickBooks):
    def configure(self):
            self.url = 'https://quickbooks.api.intuit.com/v3/company/492414475/query'
            self.target_database = 'rightatschool_testdb'
            self.target_table = 'DimAccount'
            self.source_table = ''
            self.source_database = ''
            self.object_key = 'Account'
            self.headers = {
                'Accept': 'application/json',
                #Read this Key from Configuration
                'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..1cCnQxXQJF6HJwDHvFjSqg.FjCHjyl4aRj4PHnhRXsclcaK_mMZ5T9_pyzlx1eRB1zDTtYkiJnTJu-TvADSyK3hQq2gYQXXZvUuriF8PxJoFLxlw1gLj3gstXXKkuZ6T4KmZKWE1ercGDac3tNXmRHjNsYp7tGeRoHIap235YS2MGXvmG_E6aHyv4AmbKI2CvZ3XUCVrt96gdLaSjqba8xdoshzXpsp4kOFYLvFb3BeklmJLpDfxqbmgdSuxCwEzMHSHIKV5fY9YtnrzbMIeAfYtYB3cMLS097m7n7P-_6YlKkiD8WaHJ856XIJSrHWs9d63FIfgOpjvPvSKUMZmieGPB4iSAu9oS9UJSF__6hGbnvXVuc94eWEH85PcqUu6s7fdnRqeBOu7k8NoRZ4Y3zD2ioBa1qADJn3wYp1PrZl6E0X5zumGAm-SRaLsRdTxerBNX4ufw_3eQC4dArJNjUPqiYCjgHQEeR5GiOsNNZIExSGFKUcm_FUOJt0gPVlJAZb6cqfG3FN-_2XFCTgXQNoC8ZDWD7qxmwgcfRMqNhUv0FetWG6XWI48Tof0nTyuK8pwuMF2fAvmtN2RQVkaDpQfsKA4ScQkVqxL6juxlqnX6UIriVAyJ68r5hmTLwLt_cdmcndOhFsjmaHJu3Dk6fXGPOKEz-UG2CQKrvkfSsw0db5R8AFMveyY4Tjv8DHgqdV4XxDZFrKD8V3yJDiQld4D55-PDgtkzrL1ZQe6jLwGOL_kUcA5kS22Awk4BxqVts9A4nlJwjrSjJ94qar8Sor2leexP2OxnGJyOSEFthLXrK0D4nDrm4jYwU8aLJJaGv7I0fR_vTGVYxAefxaKyS8.Ue2HSoUu7TacHWPHsGIzUQ",
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
