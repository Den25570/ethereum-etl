import xlsxwriter

class XlsxExporter:

    formats = {
        'datetime': {'num_format': 'dd/mm/yy hh:mm'},
        'string': None
    }

    workbook: xlsxwriter.Workbook
    def __init__(self, fileName):
        self.workbook = xlsxwriter.Workbook(fileName)

    def exportToXlsxAsCols(self, sheetName, array, pretty = False, labels = None):
        worksheet = self.workbook.add_worksheet(sheetName)

        row = 0
        if (pretty):
            worksheet.write_row(0, 0, labels)
            row = 1
        for col, data in enumerate(array):
            stringData = [str(item) for item in data]
            worksheet.write_column(row, col, stringData)

    def exportToXlsxAsRows(self, sheetName, array, pretty = False, labels = None):
        worksheet = self.workbook.add_worksheet(sheetName)

        col = 0
        skipRows = 0
        if (pretty):
            worksheet.write_row(0, 0, labels)
            skipRows = 1
        for row, data in enumerate(array):
            stringData = [str(item) for item in data]
            worksheet.write_row(row + skipRows, col, stringData)

    def close(self):
        self.workbook.close()