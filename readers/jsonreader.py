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
