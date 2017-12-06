from base.jobs import JSONJob, JSONCypherWorxJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension

#User from Cyperworx ->> DimEmployee in database

# class for customer_controller dimension
class LOAD_DW_DimUser_Cyperworx(JSONCypherWorxJob):
    def configure(self):
        self.url = 'https://collabornation.net/lms/api/0.06/user.json'
        self.auth_user = 'Right At School'
        self.auth_password = '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'
        self.data = 'user'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimEmployee'
        self.source_table = ''
        self.source_database = ''
        #self.file_name = ''

    def getColumnMapping(self):
        return [
                'id',
                'firstname',
                'lastname',
                'email',
                'extra_registration'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
                'EmployeeId',
                'id',
                'firstname',
                'MiddleName',
                'lastname',
                'email',
                'EffectiveDate',
                'LocationName',
                'DepartmentName',
                'RateType',
                'PayRate',
                'JobTitle',
                'EmploymentStatus',
                'HireDate',
                'extra_registration'
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
        databasefieldvalues = [
            'EmployeeId',
            'UserId',
            'FirstName',
            'MiddleName',
            'LastName',
            'Email',
            'EffectiveDate',
            'LocationName',
            'DepartmentName',
            'RateType',
            'PayRate',
            'JobTitle',
            'EmploymentStatus',
            'HireDate',
            'Team',
            'Role'
        ]

        row["Team"] = row["extra_registration"]["The Right At School team I belong to is:"]
        row["Role"] = row["extra_registration"]["My role at Right At School is: (please choose accurately)"]
        del row["extra_registration"]

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        print('NAMES', name_placeholders)
        print('VALUES', value_placeholders)

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

