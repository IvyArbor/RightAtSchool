from base.jobs import SFTCSVJob, CSVJob
from helpers.time import getTimeId
import csv
from datetime import datetime, timedelta
from dateutil import parser


# class for customer dimension
class LOAD_DW_DimApplicant(SFTCSVJob):
    def configure(self):
        self.target_database = 'rightatschool_productiondb'
        self.target_table = 'DimApplicant'
        self.delimiter = ","
        self.quotechar = '"'
        self.source_table = ''
        self.source_database = ''
        #self.file_name = "Applicant History/12112017.csv"
        self.ignore_firstline = True

        self.location_map = self._getLocationMap()

        self.sftp = "ATS"
        self.file_path = self._getFilePath()

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
                'DistrictNCESID',
                'LastState'
            ]

            if row["Req Location"][0] == 'z':
                row["Req Location"] = row["Req Location"][1:]

            row["ReqLocationState"] = row["Req Location"].split("-")[0].strip()
            row["ReqLocationDistrict"] = row["Req Location"].split("-")[1].strip()
            if row["Req Location Code"] == "" or row["Req Location Code"] == None:
                row["Req Location Code"] = self._getLocationCode(row["Req Location"])
            del row["Req Location"]
            row["LastState"] = 1

            row["Entry Date"] = parser.parse(row["Entry Date"])
            row["Activity Date"] = parser.parse(row["Activity Date"])

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            value_placeholders = ", ".join(['%s'] * len(row))

            result = self._checkRow(cursor, row)
            if result == 1:
                self._updateApplicant(cursor, row)
            if result == 0:
                row["LastState"] = 0

            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

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
            SELECT `ApplicantId`, `ActivityDate`
            FROM `DimApplicant`
            WHERE `ApplicantId` = %s AND `LastState` = 1
            LIMIT 1
        """

        cursor.execute(query, (row["Applicant ID"]))

        query_result = cursor.fetchone()
        if query_result == None:
            return None
        else:
            if query_result[1] < row["Activity Date"]:
                return 1
            return 0

    def _updateApplicant(self, cursor, row):
        row2 = {"LastState": 0}
        sql = 'UPDATE {} SET {} WHERE `ApplicantId`={}'.format(self.target_table, ', '.join('{}=%s'.format(k) for k in row2), row["Applicant ID"])
        cursor.execute(sql, tuple(row2.values()))

    def _getFilePath(self):
        d = datetime.now()
        month = d.month
        if len(str(d.month)) == 1:
            month = "0{}".format(d.month)

        day = d.day
        if len(str(d.day)) == 1:
            day = "0{}".format(d.day)

        return "/{}{}{}.csv".format(month, day, d.year)
