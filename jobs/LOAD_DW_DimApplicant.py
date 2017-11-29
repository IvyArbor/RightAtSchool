from base.jobs import CSVJob
from helpers.time import getTimeId

# class for customer dimension
class LOAD_DW_DimApplicant(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimApplicant'
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/11272017.csv'

    def getColumnMapping(self):
        return [
            'Applicant ID',
            'First Name',
            'Middle Name',
            'Last Name',
            'Applicant Status',
            'Source',
            'Specific Source',
            'Entry Date',
            'Activity Date',
            'Req Number',
            'Req Title',
            'Position Title',
            'Req Location',
            'Req Location Code'
        ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Applicant ID',
            'First Name',
            'Middle Name',
            'Last Name',
            'Applicant Status',
            'Source',
            'Specific Source',
            'Entry Date',
            'Activity Date',
            'Req Number',
            'Req Title',
            'Position Title',
            'Req Location',
            'ReqLocationState',
            'ReqLocationDistrict',
            'Req Location Code'
        ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }

        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        if row["First Name"] != "First Name":
            databasefieldvalues = [
                'ApplicantId',
                'FirstName',
                'MiddleName',
                'LastName',
                'ApplicationStatus',
                'Source',
                'SpecificSource',
                'EntryDate',
                'ActivityDate',
                'ReqNumber',
                'ReqTitle',
                'PositionTitle',
                'ReqLocationState',
                'ReqLocationDistrict',
                'ReqLocationCode'
            ]

            row["ReqLocationState"] = row["Req Location"].split("-")[0].strip()
            row["ReqLocationDistrict"] = row["Req Location"].split("-")[1].strip()
            del row["Req Location"]

            row["Entry Date"] = getTimeId(cursor, self.target_connection, row["Entry Date"])
            row["Activity Date"] = getTimeId(cursor, self.target_connection, row["Activity Date"])

            print("ROW:")
            print(row)

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders,value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
