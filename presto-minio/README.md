# Presto & Minio on Docker

This is an updated version of the presto-minio project from 
https://github.com/starburstdata/presto-minio
library.

Download the following files from here

http://www.congiu.net/hive-json-serde/1.3.8/cdh5/

Using `docker-compose` you set up Presto, Hadoop, and Minio containers for Presto to query data from Minio. Presto uses the Hadoop container for the metastore.

## Start the Docker containers

1. Get the status of the Docker daemon

```
sudo service docker status
```

2. Start the docker daemon

```
sudo service docker start
```

3. Start the Docker services

```
docker-compose up -d
```

4. In WSL2 get the ip addr

```
ip -4 addr show eth0
```

View the Minio Browser at `http://127.0.0.1:9000/`
View the Presto WebUI at `http://127.0.0.1:8080/`

Use `docker exec -it presto presto-cli` to connect to Presto.

## Stopping

Run `docker-compose stop`


## Example

First create a table in the Hive metastore. Note that the location
`'s3a://customer-data-text/'` points to data that already exists in the Minio
container.

Run `docker exec -it hadoop-master /bin/bash`. 

```
[root@hadoop-master /]# su - hdfs
-bash-4.1$ hive
hive> use default;
hive> create external table customer_text(id string, fname string, lname string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3a://customer-data-text/';
hive> select * from customer_text;
```

Next let's query the data from Presto.

Run `docker exec -it presto presto-cli`

```
presto> use minio.default;
presto:default> show tables;
	
presto:default> show tables;
     Table     
---------------  
 customer_text 
(2 rows)

presto:default> select * from customer_text;
 id | fname | lname 
----+-------+-------
 5  | Bob   | Jones 
 6  | Phil  | Brune 
(2 rows)
```

Next, let's create a new table via Presto and copy the CSV data into ORC
format. Before you do that, make a new bucket in Minio named
`customer-data-orc`.

```
presto:default> create table customer_orc(id varchar,fname varchar,lname varchar) with (format = 'ORC', external_location = 's3a://customer-data-orc/');
CREATE TABLE

presto:default> insert into customer_orc select * from customer_text;
INSERT: 2 rows

presto:default> select * from customer_orc;
 id | fname | lname 
----+-------+-------
 5  | Bob   | Jones 
 6  | Phil  | Brune
```
