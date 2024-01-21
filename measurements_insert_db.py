import MySQLdb
import config
import pandas as pd
import os
import numpy as np
from _datetime import date
from datetime import datetime
import csv

folders = config.folders
actual_date = date.today()
file_list = os.listdir(folders['input_folder2'])
#print(file_list)

mysql_config = config.mysql

mysql_conn = MySQLdb.connect(host=mysql_config['host'],
                             user=mysql_config['user'],
                             passwd=mysql_config['passwd'],
                             db=mysql_config['db'])
cur = mysql_conn.cursor()
order_id=1



for file in file_list:
    file_path = folders['input_folder2'] + '/' + file
    #destination_file_path = folders['destination_folder'] + '/' + file
    print(file)
    measure_data = np.loadtxt(file_path, delimiter=',')
    #print(measure_data)

    with open(f'csv1/{file}', 'r', newline='') as source:
        reader = csv.reader(source, delimiter=',')
        # for row in reader:
        #     toDate = datetime.strptime(row[0], '%y%m%d')
        #     print(toDate)

        for measure_value in reader:
            query = f'INSERT INTO measurements(m_id,m_date, m_hour, m_value_1, m_value_2) VALUES({order_id},{measure_value[0]},{measure_value[1]},{measure_value[2]},{measure_value[3]})'
            cur.execute(query)
            mysql_conn.commit()
            print("Executing: " + query)
            order_id +=1

    destination_file_path = folders['destination_folder'] + '/' + file[9:-6]
    print(destination_file_path)
    if not os.path.exists(destination_file_path):
        os.makedirs(destination_file_path)
    os.rename(file_path, destination_file_path + '/' + file)

