from base.jobs import JSONJob
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension

class LOAD_DW_DimHowIntroduced(JSONJob):

    def configure(self):
        self.url = 'https://api.pipedrive.com/v1/organizationFields?api_token=5119919dca43c62ca026750611806c707f78a745'
        self.auth_user = 'Right At School'
        self.auth_password = 'api_token=5119919dca43c62ca026750611806c707f78a745'
        self.object_key = 'data'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimHowIntroduced'
        self.source_table = ''
        self.source_database = ''

    def getColumnMapping(self):
        return [
            'id',
            'key',
            'name',
            'order_nr',
            'field_type',
            'add_time',
            'update_time',
            'active_flag',
            'edit_flag',
            'index_visible_flag',
            'details_visible_flag',
            'add_visible_flag',
            'important_flag',
            'bulk_edit_allowed',
            'searchable_flag',
            'options',
            'mandatory_flag'
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        myfields = [
            'id',
            'HowIntroduced',
            'options'
            ]
        newrow = {}
        for f in myfields:
            newrow[f] = row[f] if f in row else None
            #print("newrow: ",newrow)
        return newrow

    # Override the following method if the data needs to be transformed before insertion
    def insertRow(self, cursor, row):
        databasefieldvalues = [
            'HowIntroducedId',
            'HowIntroduced'
        ]
        if row["id"] == 4013:
            for option in row["options"]:
                temp_row = {"id": option["id"],
                            "HowIntroduced": option["label"]
                            }

                name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
                print("Fields:",name_placeholders)

                value_placeholders = ", ".join(['%s'] * len(databasefieldvalues))
                print(" Values:",value_placeholders)
                #print (tuple(temp_row.values()))

                sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
                cursor.execute(sql, tuple(temp_row.values()))
                self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()
