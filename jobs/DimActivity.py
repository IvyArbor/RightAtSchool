from base.jobs import CSVJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension
from datetime import datetime
from dateutil import parser
import math

# class for customer dimension
class DimActivity(CSVJob):
    def configure(self):
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimActivity'
        self.delimiter = ","
        self.quotechar = '"'
        # self.pick_file_to_process(folder = None, pattern = 'LDI_reject_claim_detail_06_05_2017.txt')
        # self.bucket_name = 'ldi.datafile.to-process'
        # self.bucket_folder = 'Rebate'
        self.source_table = ''
        self.source_database = ''
        self.file_name = 'sources/ActivityData.csv'

    def getColumnMapping(self):
        return [
            'Activity Category',
            'Activity Name',
            'Activity Number',
            'Activity Status',
            'Activity Type',
            'Customer ID',
            'Days of Week',
            'End Date',
            'End Time',
            'Organization',
            'Season',
            'Site',
            'Start Date',
            'Start Time',
            'Transaction Date',
            'Transaction Type',
            'Week of Month',
            'Amount',
            'Amount Incl Tax',
            'Total Enrolled',
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
            'Activity Number',
            'Activity Name',
            'Activity Category',
            'Activity Status',
            'Activity Type',
            'Days of Week',
            'End Date',
            'End Time',
            'Organization',
            'Season',
            'Site',
            'Start Date',
            'Start Time',
            'Transaction Date',
            'Transaction Type',
            'Week of Month'
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
        if row["Activity Number"] != "Activity Number":
            databasefieldvalues = [
                'ActivityNumber',
                'ActivityName',
                'ActivityCategory',
                'ActivityStatus',
                'ActivityType',
                'DaysOfWeek',
                'EndDate',
                'EndTime',
                'Organization',
                'Season',
                'Site',
                'StartDate',
                'StartTime',
                'TransactionDate',
                'TransactionType',
                'WeekOfMonth'
            ]

            row["Activity Number"] = int(row["Activity Number"])
            row["End Date"] = self.getTimeId(cursor, row["End Date"])
            row["Start Date"] = self.getTimeId(cursor, row["Start Date"])
            row["Transaction Date"] = self.getTimeId(cursor, row["Transaction Date"])

            name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
            print(name_placeholders)
            value_placeholders = ", ".join(['%s'] * len(row))
            print(value_placeholders)
            
            sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
            cursor.execute(sql, tuple(row.values()))
            self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

    def getTimeId(self, cursor, value):
        query = """
            SELECT `TimeId`
            FROM `DimTime`
            WHERE `Year` = %s AND `DayOfYear` = %s AND `Quarter` = %s AND `Month` = %s
            AND `MonthName` = %s AND `DayOfMonth` = %s AND `Week` = %s AND `DayOfWeek` = %s
            AND `CalendarDate` = %s AND `DateTimeStamp` = %s
            ORDER BY `TimeId` DESC
            LIMIT 1
        """

        result = self.parseTime(value)
        cursor.execute(query, (result["Year"], result["DayOfYear"], result["Quarter"], result["Month"], result["MonthName"],
                  result["DayOfMonth"], result["Week"], result["DayOfWeek"], result["CalendarDate"],
                  result["DateTimeStamp"]))

        query_result = cursor.fetchone()
        if query_result == None:
            return self.insertTime(cursor, value)
        else:
            return query_result[0]

    def insertTime(self, cursor, value):
        databasefieldvalues = ["Year", 'DayOfYear', 'Quarter', 'Month', 'MonthName', 'DayOfMonth', 'Week', 'DayOfWeek', 'CalendarDate', 'DateTimeStamp']

        result = self.parseTime(value)
        values = [result["Year"], result["DayOfYear"], result["Quarter"], result["Month"], result["MonthName"], result["DayOfMonth"], result["Week"], result["DayOfWeek"], result["CalendarDate"], result["DateTimeStamp"]]

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(values))
        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format("DimTime", name_placeholders, value_placeholders)

        cursor.execute(sql, tuple(values))
        self.target_connection.commit()
        inserted_id = cursor.lastrowid

        return inserted_id


    def parseTime(self, dt):
        date = parser.parse(dt)

        result = {}
        result["Year"] = date.year
        result["DayOfYear"] = date.timetuple().tm_yday
        result["Quarter"] = int(math.ceil(date.month / 3.))
        result["Month"] = date.month
        result["MonthName"] = date.strftime('%B')
        result["DayOfMonth"] = date.timetuple().tm_mday
        result["Week"] = date.isocalendar()[1]
        # DayOfWeek can have values from 1 to 7 (1 for Monday - 7 for Sunday)
        result["DayOfWeek"] = date.weekday() + 1
        result["CalendarDate"] = date
        # I don't like this part
        result["DateTimeStamp"] = (date - datetime(1970, 1, 1)).total_seconds()
        return result
