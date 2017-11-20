from io import StringIO

class JSONReader(object):
    '''iterator that reads a text file line by line'''

    def __init__(self, response, column_mapping, object_key):
        self.response = response
        self.column_mapping = column_mapping
        self.object_key = object_key

    def rows(self):
        for line in self.response.json()[self.object_key]:
            lst = [val if val and val != 'NULL' else None for val in list(line.values())]
            yield dict(zip(self.column_mapping, lst))

class JSONReaderCypherWorx(object):
    '''iterator that reads a text file line by line'''

    def __init__(self, response, column_mapping, data):
        self.response = response
        self.column_mapping = column_mapping
        self.data = data

    def rows(self):
        if self.data == "course":
            for line in self.response.json()["course"]:
                lst = [val if val and val != 'NULL' else None for val in list(line.values())]
                yield dict(zip(self.column_mapping, lst))
        elif self.data == "user":
            for key in self.response.json()["user"]:
                lst = [val if val and val != 'NULL' else None for val in list(self.response.json()["user"][key].values())]
                yield dict(zip(self.column_mapping, lst))
        elif self.data == "record":
            for user in self.response.json()["user"]:
                for record in self.response.json()["user"][user]["record"]:
                    lst = [val if val and val != 'NULL' else None for val in list(self.response.json()["user"][user]["record"][record].values())]
                    lst.insert(0, record)
                    lst.insert(1, user)
                    lst.insert(2, self.response.json()["user"][user]["record"][record]["course"]["id"])
                    yield dict(zip(self.column_mapping, lst))

class JSONReaderQuickBooks(object):
    '''iterator that reads a text file line by line'''

    def __init__(self, response, column_mapping, object_key):
        self.response = response
        self.column_mapping = column_mapping
        self.object_key = object_key

    def rows(self):
        for line in self.response.json()["QueryResponse"][self.object_key]:
            for column in self.column_mapping:
                if column not in line:
                    line[column] = None
            yield dict(line)
            #lst = [val if val and val != 'NULL' else None for val in list(line.values())]
            #yield dict(zip(self.column_mapping, lst))


