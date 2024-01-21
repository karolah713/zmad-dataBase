import MySQLdb
import config
import pandas as pd
import os
import numpy as np
from _datetime import date

folders = config.folders
actual_date = date.today()
file_list = os.listdir(folders['input_folder'])
print(file_list)

mysql_config = config.mysql

mysql_conn = MySQLdb.connect(host=mysql_config['host'],
                             user=mysql_config['user'],
                             passwd=mysql_config['passwd'],
                             db=mysql_config['db'])
cur = mysql_conn.cursor()

for file in file_list:
    file_path = folders['input_folder'] + '/' + file
    destination_file_path = folders['destination_folder'] + '/' + file
    print(file)
    height_data = np.loadtxt(file_path, delimiter=',')
    for height_value in height_data:
        #query = f'INSERT INTO person(height, added_date) VALUES("{height_value}","{actual_date}")'
        query = "INSERT INTO person(height, added_date) VALUES("+str(height_value)+",'"+str(actual_date)+"')"
        cur.execute(query)
        mysql_conn.commit()
        print("Executing: " + query)
    os.rename(file_path, destination_file_path)