import csv

def readCsv(paths):
    data = []
    col_names= []

    for path in paths:
        with open(path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if (len(row) > 0):
                    if line_count == 0:
                        col_names = row
                    else:
                        data.append(dict(zip(col_names, row)))
                    line_count += 1
            print(f'Processed {line_count} lines.')
    return data, col_names

def concatCsvs(paths, output):
    data, col_names = readCsv(paths)
    with open(output, 'w+') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(col_names)
        for col in data:
            csv_writer.writerow(col.values()) 
    return data