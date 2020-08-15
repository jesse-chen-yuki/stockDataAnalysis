import os

from clickhouse_driver import Client
import mysql.connector
from csv import DictReader
from datetime import datetime

def iter_csv(filename):
     converters = {
         'qty': int,
         'time': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
     }

     with open(filename, 'r') as f:
         reader = DictReader(f)
         for line in reader:
             yield {k: (converters[k](v) if k in converters else v) for k, v in line.items()}


client = Client(host='localhost', password='ABCDE', database='shenzhendata')

client.execute(
     'CREATE TABLE IF NOT EXISTS data_csv '
     '('
         'time DateTime, '
         'order String, '
         'qty Int32'
     ') Engine = Memory'
 )
client.execute('truncate table data_csv')
a = iter_csv('data.csv')
print(a)
client.execute('INSERT INTO data_csv VALUES', iter_csv('data.csv'))

print(client.execute('select * from data_csv'))

import csv
#
# raw_path = "../../../PythonProj/raw"
#
# for (dirpath, dirnames, files) in os.walk(raw_path):
#     for file_name in files:
#         inf = open(dirpath + "/" + file_name, "r")
#
#         reader = csv.reader(inf, delimiter=" ")
#         second_col = list(zip(*reader))[1]
#         print(second_col)
#         f.close()

with open('data1.csv') as inf:
    reader = csv.reader(inf, delimiter=" ")
    ncol = len(next(reader))  # Read first line and count columns
    inf.seek(0)
    print(ncol)

    # print out each column
    col = zip(*reader)
    for i in col:
        print(i)
