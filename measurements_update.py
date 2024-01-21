import MySQLdb
import config
import os
import numpy as np
from _datetime import date
import csv

folders = config.folders

mysql_config = config.mysql

mysql_conn = MySQLdb.connect(host=mysql_config['host'],
                             user=mysql_config['user'],
                             passwd=mysql_config['passwd'],
                             db=mysql_config['db'])
cur = mysql_conn.cursor()

query2 = 'ALTER TABLE measurements ADD m_value_3 DOUBLE(10,2)'
# cur.execute(query2)
# mysql_conn.commit()

file_list = os.listdir(folders['input_folder3'])
#print(file_list_csv2)


for file in file_list:
    file_path = folders['input_folder3'] + '/' + file
    destination_file_path = folders['destination_folder'] + '/' + 'csv_processed' + '/' + file
    #print(file)
    measure_data = np.loadtxt(file_path, delimiter=',')
    #print(measure_data)

    with open(f'csv2/{file}', 'r', newline='') as source:
        reader = csv.reader(source, delimiter=',')
        # for row in reader:
        #     #toDate = datetime.strptime(row[0], '%y%m%d')
        #     print(row[2])

        for measure_value in reader:
            query = f'UPDATE measurements SET m_value_3 = {measure_value[2]} WHERE m_date={measure_value[0]} and m_hour={measure_value[1]}'
            cur.execute(query)
            mysql_conn.commit()
            print("Executing: " + query)

    os.rename(file_path, destination_file_path)