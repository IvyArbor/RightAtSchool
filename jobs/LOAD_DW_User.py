from base.jobs import JSONJob

class LOAD_DW_User(JSONJob):
    def configure(self):
        self.url = 'https://api.pipedrive.com/v1/users?api_token=5119919dca43c62ca026750611806c707f78a745'
        self.auth_user = 'Right At School'
        self.auth_password = 'https://api.pipedrive.com/v1/users?api_token=5119919dca43c62ca026750611806c707f78a745'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimUser'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "data"
        #self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
                "id",
                "name",
                "default_currency",
                "locale",
                "lang",
                "email",
                "phone",
                "activated",
                "last_login",
                "created",
                "modified",
                "signup_flow_variation",
                "has_created_company",
                "is_admin",
                "timezone_name",
                "timezone_offset",
                "active_flag",
                "role_id",
                "icon_url",
                "is_you"
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    def prepareRow(self, row):
        myfields = [
                "id",
                "name",
                "default_currency",
                "locale",
                "lang",
                "email",
                "phone",
                "activated",
                "last_login",
                "created",
                "modified",
                "signup_flow_variation",
                "has_created_company",
                "is_admin",
                "timezone_name",
                "timezone_offset",
                "active_flag",
                "role_id",
                "icon_url",
                "is_you"
            ]
        # print(myfields)
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
        # newrow = { f:row[f] for f in set(myfields) }

        #print('new:', newrow)

        return newrow

    def insertRow(self, cursor, row):
        databasefieldvalues = [
            "UserId",
            "Name",
            "DefaultCurrency",
            "Locale",
            "Lang",
            "Email",
            "Phone",
            "Activated",
            "LastLogin",
            "Created",
            "Modified",
            "SignupFlowVariation",
            "HasCreatedCompany",
            "IsAdmin",
            "TimezoneName",
            "TimezoneOffset",
            "ActiveFlag",
            "RoleId",
            "IconUrl",
            "IsYou"
        ]

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
