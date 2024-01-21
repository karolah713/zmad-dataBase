import cassandra.cluster as cluster

cluster = cluster.Cluster()
connection = cluster.connect('customers')
results = connection.execute('SELECT * FROM customer')
for row in results:
    print('Name of customer:', row.customer_name)
    print('City of customer:', row.city)
    print(row)
