import pymysql
import pygrametl

from pygrametl.datasources import CSVSource
from pygrametl.tables import Dimension

#configure database
dbconn = pymysql.connect(database="rightatschool", host = '127.0.0.1',user="root", password="mysql111")

cursor = dbconn.cursor()

#drop table if exists

cursor.execute("DROP TABLE IF EXISTS customer_dim")

#create table customer_dim


sql = """CREATE TABLE customer_dim(
customer_id INT NOT NULL,
customertype_id VARCHAR (20),
geographic_area_id VARCHAR (100),
lastpayee_id INT,
site_id	VARCHAR (20),
grade_id VARCHAR (20),
firstname CHAR (20),
lastname CHAR (20)
)"""

#execute query
cursor.execute(sql)
dbconn.commit()

#connect pygrametl
etl_connection = pygrametl.ConnectionWrapper(dbconn)
#connection.setasdefault()

#name_mapping = 'customer_id','customertype_id','geographic_area_id','lastpayee_id','site_id', 'grade_id','firstname','lastname'



#dimension object
customer_dim = Dimension(
    name='customer_dim',
    key='customer_id',
    attributes=['customertype_id','geographic_area_id','lastpayee_id','site_id' , 'grade_id','firstname','lastname'],
    lookupatts=['customer_id'])

csv_source = CSVSource(open('C:/Users/Nurane/PycharmProjects/Customers_Extract_Modified.csv', 'r'), delimiter=',')

for row in csv_source:
    customer_dim.insert(row)
etl_connection.commit()


'''
def load_customer_dim():
    customer = CSVSource(open('C:/Users/Nurane/PycharmProjects/Customers_Extract_Modified.csv', 'r'), delimiter = ',')
    for row in customer:
        customer_dim.insert(row)
        print(row)
    etl_connection.commit()

def main():
    load_customer_dim()

'''
etl_connection.close()

