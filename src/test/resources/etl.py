import csv
import os

#input_folder = "./trainRawData"
#output_file = "train.csv"
input_folder = "./testRawData"
output_file = "test.csv"


with open(output_file, 'w') as csvfile:
    output_csv = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)


    def _save_row(args):
        output_csv.writerow(args)


    for file in os.listdir(input_folder):
        _file_prefix = input_folder + "./"
        _o = open(_file_prefix + file, "r")
        _l = len(_file_prefix)
        _file_prefix = str(_o.name[_l:_l + 1])
        _save_row([_file_prefix, _o.read()])
