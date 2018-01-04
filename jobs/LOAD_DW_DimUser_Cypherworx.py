from base.jobs import JSONJob, JSONCypherWorxJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension

#User from Cyperworx ->> DimEmployee in database

# class for customer_controller dimension
class LOAD_DW_DimUser_Cypherworx(JSONCypherWorxJob):
    def configure(self):
        self.url = 'https://collabornation.net/lms/api/0.06/user.json'
        self.auth_user = 'Right At School'
        self.auth_password = '29AD1B22C11CA60D3CB34D65C063103636F0D35D65ED45A751F98FC5C1CA293C'
        self.data = 'user'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'TestDimEmployee'
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
                'NCESId',
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
            'NCESId',
            'Team',
            'Role'
        ]

        registrationValues = {
            "The Right At School team I belong to is:": row["extra_registration"]["The Right At School team I belong to is:"],
            "My role at Right At School is: (please choose accurately)": row["extra_registration"]["My role at Right At School is: (please choose accurately)"]
        }

        for key, value in registrationValues.items():
            try:
                if key == "The Right At School team I belong to is:":
                    row["Team"] = row["extra_registration"]["The Right At School team I belong to is:"]
                else:
                    row["Team"] = None

                if key == "My role at Right At School is: (please choose accurately)":
                    row["Role"] = row["extra_registration"]["My role at Right At School is: (please choose accurately)"]
                else:
                    row["Role"] = None

                if key[0] == '' and key[1] == '':
                    row["Team"] = None
                    row["Role"] = None

            except KeyError:
                print('An exception happened!')




        # if  row["extra_registration"] == None:
        #     row["Team"]= None
        #     row["Role"]= None
        #
        # else:
        #     try:
        #         keys = list(row["extra_registration"].keys())
        #         key0 = 'The Right At School team I belong to is:'
        #         key1 = 'My role at Right At School is: (please choose accurately)'
        #         #case when we have both Team and Role
        #         if keys[0] == key0 and keys[1]==key1:
        #             row["Team"] = row["extra_registration"]["The Right At School team I belong to is:"]
        #             row["Role"] = row["extra_registration"]["My role at Right At School is: (please choose accurately)"]
        #         else:
        #             #case only when we have Role
        #             if keys[0] == key1:
        #                 row["Team"] = ""
        #                 row["Role"] = row["extra_registration"]["My role at Right At School is: (please choose accurately)"]
        #     except IndexError:
        #         #case only when we have Team
        #         row["Team"] = row["extra_registration"]["The Right At School team I belong to is:"]
        #         row["Role"] = ""


        del row["extra_registration"]

        if self._checkRow(cursor, row) == None:
            self.insertDict(cursor, row, self.target_table, databasefieldvalues)
        else:
            self.updateDict(cursor, row, self.target_table, databasefieldvalues)
        self.target_connection.commit()

    def insertDict(self, cursor, row, table_name, databasefieldvalues):
        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        print("Row to be inserted in ", table_name)
        print(row)
        # insert values
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(table_name, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))

    def updateDict(self, cursor, row, table_name, databasefieldvalues):
        print("Row to be updated in ", table_name)
        print(row)
        sql = 'UPDATE {} SET {} WHERE `UserId`={}'.format(table_name, ', '.join(
            '{}=%s'.format(k) for k in databasefieldvalues), row["id"])
        cursor.execute(sql, tuple(row.values()))

    def _checkRow(self, cursor, row):
        query = """
             SELECT `UserId`
             FROM `TestDimEmployee`
             WHERE `UserId` = %s
             LIMIT 1
         """

        cursor.execute(query, (row["id"]))

        query_result = cursor.fetchone()
        if query_result == None:
            return None
        else:
            return 1

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()

