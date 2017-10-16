class TextReader(object):
    '''Reads lines of a file and splits them to the map'''

    def __init__(self, stream, column_mapping):
        self.stream = stream
        self.column_mapping = column_mapping

    def rows(self):
        for line in self.stream.lines():
            result = {}
            for (name, start, end) in self.column_mapping:
                val = line[start:end].strip()
                result[name] = val
            yield result
