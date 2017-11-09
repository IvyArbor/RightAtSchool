from base.jobs import JSONJob
from helpers.time import getTimeId


# class for customer_controller dimension
class LOAD_DW_Stage(JSONJob):
    def configure(self):
        self.url = 'https://api.pipedrive.com/v1/stages?api_token=5119919dca43c62ca026750611806c707f78a745'
        self.auth_user = 'Right At School'
        self.auth_password = 'https://companydomain.pipedrive.com/v1/persons/?api_token=5119919dca43c62ca026750611806c707f78a745'
        self.target_database = 'rightatschool_testdb'
        self.target_table = 'DimStage'
        self.source_table = ''
        self.source_database = ''
        self.object_key = "data"
        #self.file_name = 'sources/ActivityEnrollmentSample.csv'

    def getColumnMapping(self):
        return [
                "id",
                "order_nr",
                "name",
                "active_flag",
                "deal_probability",
                "pipeline_id",
                "rotten_flag",
                "rotten_days",
                "add_time",
                "update_time",
                "pipeline_name"
            ]

    def getTarget(self):
        # print('target')
        return self.target_connection.cursor()

    # Override the following method if the data needs to be transformed before insertion
    def prepareRow(self, row):
        # print('prepare')
        myfields = [
                "id",
                "order_nr",
                "name",
                "active_flag",
                "deal_probability",
                "pipeline_id",
                "rotten_flag",
                #"rotten_days",
                "add_time",
                "update_time",
                "pipeline_name"
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
        databasefieldvalues = [
            "StageId",
            "OrderNumber",
            "Name",
            "ActiveFlag",
            "DealProbability",
            "PipelineId",
            "RottenFlag",
            "AddTime",
            "UpdateTime",
            "PipelineName"
        ]

        row["add_time"] = getTimeId(cursor, self.target_connection, row["add_time"])
        row["update_time"] = getTimeId(cursor, self.target_connection, row["update_time"])

        name_placeholders = ", ".join(["`{}`".format(s) for s in databasefieldvalues])
        value_placeholders = ", ".join(['%s'] * len(row))

        sql = "INSERT INTO `{}` ({}) VALUES ({}) ".format(self.target_table, name_placeholders, value_placeholders)
        cursor.execute(sql, tuple(row.values()))
        self.target_connection.commit()

    def close(self):
        """Here we should archive the file instead"""
        # self.active_cursor.close()