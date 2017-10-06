import csv
from io import StringIO

class CSVReader(object):
    '''iterator that reads a text file line by line'''

    def __init__(self, stream, column_mapping, delimiter, quotechar):
        self.stream = stream
        self.column_mapping = column_mapping
        self.delimiter = delimiter
        self.quotechar = quotechar

    def rows(self):
        for line in self.stream.lines():
            buffer = StringIO(line)
            reader = csv.reader(buffer, delimiter=self.delimiter, quotechar=self.quotechar)
            for line in reader:
                lst = [val if val and val != 'NULL' else None for val in line]
                yield dict(zip(self.column_mapping, lst))
