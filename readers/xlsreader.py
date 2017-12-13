from xlrd import open_workbook

class XlsReader(object):
    '''iterator that reads an excel file line by line'''

    def __init__(self, reader, sheet_name, col_names=None, first_data_row = 2):
        self.col_names = col_names
        self.reader = reader
        # self.workbook = load_workbook('sources/Quickbooks_FactFinance.xlsx', data_only=True)
        self.workbook = open_workbook(filename = reader.getFile())
        #Specifying the sheet name we want to read.
        self.sheet = self.workbook.sheet_by_name(sheet_name)
        self.max_row = self.sheet.nrows
        self.max_col = len(self.col_names)
        self.first_data_row = first_data_row - 1

    def rows(self):
        for row in range(self.first_data_row, self.max_row):
            result = self.readRow(row)
            for val in result.values():
                if val is not None:
                    yield result
                    break

    def readRow(self, row):
        result = {}
        for col in range(0, self.max_col):
            value = self.sheet.cell(row, col).value
            if value is None or value == "NULL" or value == "":
                value = None
            else:
                value = str(value)
            result[self.col_names[col]] = value
        return result

    def columns(self):
        return self.readRow(1)

    def close(self):
        self.workbook.close()