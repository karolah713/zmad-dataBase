import MySQLdb
import config
import pandas as pd
import pyarrow

mysql_config = config.mysql

mysql_conn = MySQLdb.connect(host=mysql_config['host'],
                             user=mysql_config['user'], passwd=mysql_config['passwd'], db=mysql_config['db'])
cur = mysql_conn.cursor()

query = 'SELECT * FROM testing'
cur.execute(query)
for row in cur:
    print(row)

df = pd.DataFrame(cur.fetchall())
df.columns = ['id','name']

mysql_conn.close()