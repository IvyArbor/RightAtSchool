from datetime import datetime
from dateutil import parser
import math


def getTimeId(cursor, target_connection, value):
    query = """
        SELECT `TimeId`
        FROM `DimTime`
        WHERE `Year` = %s AND `DayOfYear` = %s AND `Quarter` = %s AND `Month` = %s
        AND `MonthName` = %s AND `DayOfMonth` = %s AND `Week` = %s AND `DayOfWeek` = %s
        AND `CalendarDate` = %s AND `DateTimeStamp` = %s
        ORDER BY `TimeId` DESC
        LIMIT 1
    """

    if value == None:
        return None

    result = parseTime(value)
    cursor.execute(query, (result["Year"], result["DayOfYear"], result["Quarter"], result["Month"], result["MonthName"],
                           result["DayOfMonth"], result["Week"], result["DayOfWeek"], result["CalendarDate"],
                           result["DateTimeStamp"]))

    query_result = cursor.fetchone()
    if query_result == None:
        return insertTime(cursor, target_connection, value)
    else:
        return query_result[0]


def insertTime(cursor, target_connection, value):
    databasefieldvalues = ["Year", 'DayOfYear', 'Quarter', 'Month', 'MonthName', 'DayOfMonth', 'Week', 'DayOfWeek',
                           'CalendarDate', 'DateTimeStamp']

    result = parseTime(value)
    values = [result["Year"], result["DayOfYear"], result["Quarter"], result["Month"], result["MonthName"],
              result["DayOfMonth"], result["Week"], result["DayOfWeek"], result["CalendarDate"],
              result["DateTimeStamp"]]

    name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
    value_placeholders = ", ".join(['%s'] * len(values))
    sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format("DimTime", name_placeholders, value_placeholders)

    cursor.execute(sql, tuple(values))
    target_connection.commit()
    inserted_id = cursor.lastrowid

    return inserted_id


def parseTime(dt):
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