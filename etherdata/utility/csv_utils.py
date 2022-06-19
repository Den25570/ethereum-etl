import sys
import csv

def set_max_field_size_limit():
    max_int = sys.maxsize
    decrement = True
    while decrement:
        # decrease the maxInt value by factor 10
        # as long as the OverflowError occurs.

        decrement = False
        try:
            csv.field_size_limit(max_int)
        except OverflowError:
            max_int = int(max_int / 10)
            decrement = True

def read_csv(paths):
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

def concat_csvs(paths, output):
    data, col_names = read_csv(paths)
    with open(output, 'w+') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(col_names)
        for col in data:
            csv_writer.writerow(col.values()) 
    return data
