from base.jobs import JSONQuickBooks
from helpers.time import getTimeId

class LOAD_DW_DimFinanceCustomer(JSONQuickBooks):
    def configure(self):
        self.url = 'https://quickbooks.api.intuit.com/v3/company/492414475/query'

        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimFinanceCustomer'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "Customer"

        self.headers = {
            'Accept': 'application/json',
            #Read this Key from Configuration
            'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..uRqZ103IyGwDH4QirJtXqg.lTSFJ0WhKobuTIoCuPvULWpthmuYr7x-PGvjpLwFYPjDWjsVNl-i2xv9JNnko5FKGYaOKYOZIS4ZbAfv_RGjnDcJa0eWk9zoa_WXpvEl_z-XLBSHLUToAH9G8ukxsw3dDK0BhTCcGAEBrKD6HZvhNiACNJ4rjj2tyZ7sT1qMhWpaBAdu1BVZbDYL_xSCDNcuGO-bOLx9aLvJuTOosli_xDOsLTqUr6QRpey3DRivnIwuTtY4KbtMRllkVp7Kz8KjrwqRb7BJHTU3zzw1qlYaEiNSQjcaTUYBrQOKdKfOhRvpTLN4RWxcuPmA16dDIgDfRYelyebpmwpYJdHhO0Fgz2sRsa38f8velPJZ7kmQipSbeSb7IeJSvMhTa1r9CZPBBwnCzv9LrlpSzMf8JUIL1Ws0wNTQNFd73UHxWEp3qi27N0do3NIASKNHFDLV6dry2OqYpWuWOPIl41Q9LWoxFxCmIocraxxX5YuJEY2Uxy0e-jA9rXNcyGUQ9kt-wwK5267_MDHYLxtwo44SJp76J54wE498E7kUPkHJIIMOctyGvYh7Fe4WvKCE0-utNclKqj3al1pPx0uMThARC5THGcDrDIWIQwQHVND6ux86YYlIYJtsrE0MM7T4qeT3xJI1BI-YUEhTKD8xoGblex9IzBVCGEy85OnTykNNAh1n6DXsep_o835Zora5kr91_7GvTe1AqSl10VJBSFEXYQ7r0NcVKCSn6Wu1swwr_xS3YjNVip89xQe4LBpegQ_5KeGqdsgWValP48iM8T-dOQ7XgZavyZfqZSYh1ucdH5UsEooIo86QR74j3ggbeMgn2Vgz.CMz1bNzXQtd5ZTIZnfDxsw",
            'cache-control': "no-cache",
            #'User-Agent': 'python-quickbooks V3 library',
        }

        self.querystring = {"query": "SELECT * FROM Customer STARTPOSITION 1 MAXRESULTS 400", "minorversion": "4"}

    def getColumnMapping(self):
        return [
            'Id',
            'SyncToken',
            'Title',
            'GivenName',
            'MiddleName',
            'FamilyName',
            'Suffix',
            'DisplayName',
            'FullyQualifiedName',
            'CompanyName',
            'PrintOnCheckName',
            'Active',
            'PrimaryPhone',
            'AlternatePhone',
            'Mobile',
            'Fax',
            'PrimaryEmailAddr',
            'WebAddr',
            'DefaultTaxCodeRef',
            'Taxable',
            'TaxExemptionReasonId',
            'BillAddr',
            'ShipAddr',
            'Notes',
            'Job',
            'BillWithParent',
            'ParentRef',
            'Level',
            'SalesTermRef',
            'Balance',
            'OpenBalanceDate',
            'BalanceWithJobs',
            'CurrencyRef',
            'PreferredDeliveryMethod',
            'ResaleNum',
            'ARAccountRef',
            'domain',
            'sparse',
            'PaymentMethodRef',
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
            'Title',
            'GivenName',
            'MiddleName',
            'FamilyName',
            'Suffix',
            'DisplayName',
            'FullyQualifiedName',
            'CompanyName',
            'PrintOnCheckName',
            'Active',
            'PrimaryPhone',
            'AlternatePhone',
            'Mobile',
            'Fax',
            'PrimaryEmailAddr',
            'WebAddr',
            'DefaultTaxCodeRef',
            'Taxable',
            'TaxExemptionReasonId',
            'BillAddr',
            'ShipAddr',
            'Notes',
            'Job',
            'BillWithParent',
            'ParentRef',
            'Level',
            'SalesTermRef',
            'Balance',
            'OpenBalanceDate',
            'BalanceWithJobs',
            'CurrencyRef',
            'PreferredDeliveryMethod',
            'ResaleNum',
            'ARAccountRef',
            'domain',
            'sparse',
            'PaymentMethodRef',
            'MetaData'
            ]

        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        databasefieldvalues = [
            'FinanceCustomerId',
            'SyncToken',
            'Title',
            'GivenName',
            'MiddleName',
            'FamilyName',
            'Suffix',
            'DisplayName',
            'FullyQualifiedName',
            'CompanyName',
            'PrintOnCheckName',
            'Active',
            'PrimaryPhone',
            'AlternatePhone',
            'Mobile',
            'Fax',
            'PrimaryEmailAddr',
            'WebAddr',
            'DefaultTaxCodeRef',
            'Taxable',
            'TaxExemptionReasonId',
            'BillAddr',
            'ShipAddr',
            'Notes',
            'Job',
            'BillWithParent',
            'ParentRef',
            'Level',
            'SalesTermRef',
            'Balance',
            'OpenBalanceDate',
            'BalanceWithJobs',
            'CurrencyRef',
            'PreferredDeliveryMethod',
            'ResaleNum',
            'ARAccountRef',
            'Domain',
            'Sparse',
            'PaymentMethodRef',
            'CreateTime',
            'LastUpdatedTime'
        ]

        print ("ROW:")
        print (row)

        row["Id"] = int(row["Id"])
        row["PrimaryPhone"] = row["PrimaryPhone"]["FreeFormNumber"] if row["PrimaryPhone"] != None else None
        row["AlternatePhone"] = row["AlternatePhone"]["FreeFormNumber"] if row["AlternatePhone"] != None else None
        row["Mobile"] = row["Mobile"]["FreeFormNumber"] if row["Mobile"] != None else None
        row["Fax"] = row["Fax"]["FreeFormNumber"] if row["Fax"] != None else None
        row["PrimaryEmailAddr"] = row["PrimaryEmailAddr"]["Address"] if row["PrimaryEmailAddr"] != None else None
        row["WebAddr"] = row["WebAddr"]["URI"] if row["WebAddr"] != None else None
        row["DefaultTaxCodeRef"] = row["DefaultTaxCodeRef"]["name"] if row["DefaultTaxCodeRef"] != None else None
        row["BillAddr"] = row["BillAddr"]["Id"] if row["BillAddr"] != None else None
        row["ShipAddr"] = row["ShipAddr"]["Id"] if row["ShipAddr"] != None else None
        row["ParentRef"] = row["ParentRef"]["value"] if row["ParentRef"] != None else None
        row["SalesTermRef"] = row["SalesTermRef"]["value"] if row["SalesTermRef"] != None else None
        row["OpenBalanceDate"] = row["OpenBalanceDate"]["date"] if row["OpenBalanceDate"] != None else None
        row["CurrencyRef"] = row["CurrencyRef"]["value"] if row["CurrencyRef"] != None else None
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
