A Pig LoadFunc that reads all columns from a given ColumnFamily.

First build and start a Cassandra server with the default configuration*, 
then, ensuring that JAVA_HOME and PIG_CONF_DIR are set corrently, run:

contrib/pig_loadfunc$ ant
contrib/pig_loadfunc$ bin/pig_cassandra

Once the 'grunt>' shell has loaded, try a simple program like the following:
grunt> records = LOAD 'cassandra://Keyspace1/Standard1' USING CassandraStorage();
grunt> limited = LIMIT records 100;
grunt> dump limited;

*If you want to point Pig at a real cluster, modify the seed
address in storage-conf.xml accordingly.
