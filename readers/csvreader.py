import csv
from io import StringIO

class CSVReader(object):
    '''iterator that reads a text file line by line'''

    def __init__(self, stream, column_mapping, delimiter, quotechar, ignore_firstline = False):
        self.stream = stream
        self.column_mapping = column_mapping
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.ignore_firstline = ignore_firstline

    def rows(self):
        iter = self.stream.lines()

        if self.ignore_firstline:
            iter.__next__()

        for line in iter:
            line = line.replace('| ', '|')
            buffer = StringIO(line)
            reader = csv.reader(
                buffer,
                delimiter=self.delimiter,
                quotechar=self.quotechar)
            for line in reader:
                print (line)
                lst = [val if val and val != 'NULL' else None for val in line]
                yield dict(zip(self.column_mapping, lst))

    def close(self):
        pass
