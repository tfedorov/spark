import csv
import os


def do_etl(input_folder, output_file):
    with open(output_file, 'w') as csvfile:
        output_csv = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC, escapechar=",")
        output_csv.writerow(["label", "sentence"])

        def _save_row(args):
            output_csv.writerow(args)

        for file in os.listdir(input_folder):
            _file_prefix = input_folder + "./"
            _o = open(_file_prefix + file, "r")
            _l = len(_file_prefix)
            _file_prefix = float(_o.name[_l:_l + 1])
            _content = _o.read().replace('\n', ' ').replace(',', ' ')
            _save_row([_file_prefix, _content])


do_etl("./trainRawData", "train.csv")
do_etl("./testRawData", "test.csv")
