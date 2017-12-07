from base.jobs import SFTCSVJob, CSVJob
from helpers.time import getTimeId
import csv

# class for customer dimension
class LOAD_DW_DimApplicant(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimApplicant'
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/ATS/12062017.csv'
        self.ignore_firstline = True

        self.location_map = self._getLocationMap()
        #self.file_path = "/mnt/sftp-filetransfer-bucket/RightAtSchool_11202017.csv"

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
            row["Req Location Code"] = self._getLocationCode(row["Req Location"])
            del row["Req Location"]

            row["Entry Date"] = getTimeId(cursor, self.target_connection, row["Entry Date"])
            row["Activity Date"] = getTimeId(cursor, self.target_connection, row["Activity Date"])

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            if self._checkRow(cursor, row) == 1:
                self._updateApplicant(cursor, row)

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()


    def _getLocationMap(self):
        with open('sources/ATS - LocationMap.csv', 'r') as f:
            reader = csv.reader(f)
            locations = list(reader)

        return locations

    def _getLocationCode(self, location):
        for row in self.location_map:
            if location in row:
                return row[6]

        return ""

    def _checkRow(self, cursor, row):
        query = """
            SELECT `ApplicantId`
            FROM `DimApplicant`
            WHERE `ApplicantId` = %s
            LIMIT 1
        """

        cursor.execute(query, (row["Applicant ID"]))

        query_result = cursor.fetchone()
        if query_result == None:
            return None
        else:
            return 1

    def _updateApplicant(self, cursor, row):
        row2 = {"LastState": 0}
        sql = 'UPDATE {} SET {} WHERE `ApplicantId`={}'.format(self.target_table, ', '.join('{}=%s'.format(k) for k in row2), row["Applicant ID"])
        cursor.execute(sql, tuple(row2.values()))
