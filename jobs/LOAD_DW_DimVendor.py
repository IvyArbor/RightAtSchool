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
            'authorization': "Bearer eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..m-ykyYq7SrpzyJxEHcXs3g.P6oLnN34fhE6Vw_rkKWBd5CRFOcgOr_V6la_dKuC3mOhLL5_SBgJY5Nb32Zh5PFZE-ucAz-DMUGMsA4nKPL9SiilSveSOQKkWPQC0B8EYsCwJzoqpf3zttPaQ93y26GJ_tyFJq4fT_xjhNDxe78sEGDrP3ITjEh2DByFVn1SxGzg3_Ilj70-F_KxLyBjchu_4h4n2AxI6eJk5NcUJH2xqXjt8wW4W5Mki9QsYJRZVrqkrQcGXOMRXJFOyM5Nchzaj83XSr4wv_7AUF3fKr8byfMfxgU7Nqcdi9t5xFZ62VRp3I4-9OPHmjL_1SQdWxFnA98E7PlH3ovca4yfPeGbMASj76Iz2iKUv-IZPsP553hzgxKmRRKGnY0n5wVwzf4XN1Q92lZNatzBZqiWIljsPqaN3lo3TAvpR_eFsn0ysFnP3MIe1MiXrMVEPivtx2t77M8a7c0oN0QQWJ7-JyVp3U7N9nlje8oMdVLX0yGF71soEpRtI_S_QWaCtnAOtAUq27U8g6Pmk_Pmikot3GspXBaX_9jtu0RgbUd0PLD0Tjm4LasL4jKbiZrYvcYb68kwmVAhWf0Nq3kaklGz1g1abvFaCw5ZmCUmdvjO6CxgG6wvLmhrhAYZYALj_vfqKZknTwme_fwwGe-B-fnE_5BSCaaG7N5111DdxsXZu4JnuCrAmPOr1hl7KSOwkJeblxyKIocd1WXwNViB___q9iSwrjS3Nd75rjNWPrXOGGu29RRLS2KPDKZdQnnG5QulKcKM5h8XztTSkOzgXyqPzkzxNYf7IBpAmBwZui5g5Kx3A1oTj96DJ5RbtHWqY-97vxArf9jzbxVyrq0VhcIXTPS0OQ.nuCryeklWgbL27z8B1Uu6A",
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
                'Active',
                'BillAddr',
                'AcctNum'
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
                'Active',
                'BillAddr',
                'AcctNum'
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
