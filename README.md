# Presto minio

* Source code - [Github][1]
* Author - Gavin Noronha - <gavinln@hotmail.com>

[1]: https://github.com/gavinln/presto-minio

## About

This project provides a [Ubuntu (18.04)][10] [Vagrant][20] Virtual Machine (VM)
with multiple big data projects. It uses Docker containers to setup
[Hadoop][30], [Hive][40], [Presto][50], [Minio][60]. It also use Database tool
(DBT) to transform data using a ELT process.

[10]: http://releases.ubuntu.com/18.04/
[20]: http://www.vagrantup.com/
[30]: https://hadoop.apache.org/
[40]: https://github.com/apache/hive
[50]: https://prestodb.github.io/
[60]: https://min.io/

There are [Ansible][90] scripts that automatically install the software when
the VM is started.

[90]: https://www.ansible.com/

## Setup the machine

### Setup using Vagrant

All the software installed exceeds the standard 10GB size of the virtual
machine disk. Install the following plugin to resize the disk.

1. List the vagrant plugins

```
vagrant plugin list
```

2. Install the Vagrant [disksize][100] plugin

```
vagrant plugin install vagrant-disksize
```

[100]: https://github.com/sprotheroe/vagrant-disksize


3. Login to the virtual machine
```
vagrant ssh
```

4. Change to the root directory
```
cd /vagrant
```

5. Create a directory of Minio object storage (like S3) - May not be needed
```
sudo mkdir /data
sudo chown vagrant:vagrant /data
```

### Setup using WSL

1. Install pre-requisites for pyhive (Python Hive/Presto library)

```
sudo apt install libsasl2-dev 
```

### Download Presto CLI

1. Download the Presto CLI from here
https://prestodb.io/docs/current/installation/cli.html

2. Rename the file to `presto-cli.jar`

3. java -jar presto-cli.jar

## Use Presto, Minio and Hive

### Setup Presto, Minio and Hive

1. Change to the project directory
```
cd /vagrant/presto-minio-update
```

2. Add to file /vagrant/presto-minio/presto/minio.properties

```
hive.metastore-cache-ttl=0s
hive.metastore-refresh-interval = 5s
hive.allow-drop-table=true
hive.allow-rename-table=true
```

3. Start Presto and Minio
```
docker-compose up -d
```

4. View Minio at http://gavinsvr:9000/

Use the access key and secret access key from the docker-compose.yml file
There are two folders called customer-data-text and customer-data-json

5. View the Presto WebUI at http://gavinsvr:8080/

6. Any user will work. No password is necessary

7. Version software versions
```
docker-compose exec hadoop hadoop version
docker-compose exec hadoop hive --version
```

8. Start the cli

https://repo1.maven.org/maven2/io/trino/trino-cli/353/trino-cli-353-executable.jar

```
java -jar ~/presto-cli.jar
```

9. Setup parquet-cli

```
sudo pip install parquet-cli
```

10. View the parquet file details

```
parq ./minio/data/example-parquet/example.parq
```

### Create a table using Hive

1. Connect to the Hadoop master

```
docker exec -it hadoop-master /bin/bash
```

2. Start hive

```
su - hdfs
hive
```

3. Create the customer_text table

```
use default;
create external table customer_text(
    id string COMMENT 'id comment',
    fname string COMMENT 'first name',
    lname string COMMENT 'last name')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED as TEXTFILE location 's3a://customer-data-text/';
select * from customer_text;
alter table customer_text SET TBLPROPERTIES (
    'comment'='customer_text table with three columns'
);
```

4. Exit the hive cli

```
exit;
```

5. Exit the Docker container

```
exit
```

### Query the Hive table using Presto

1. Connect to Presto

```
docker exec -it presto presto-cli
```

2. Query data from Presto

```
use minio.default;
show tables;
show columns from customer_text;
show create table customer_text;
select * from customer_text;
```

3. Exit from the Presto cli

```
quit
```

4. Follow instructions at https://github.com/starburstdata/presto-minio

5. Try intake: https://intake.readthedocs.io/en/latest/index.html

### Use beeline to run queries on Hive

1. Connect to the Hadoop master

```
docker exec -it hadoop-master beeline
```

2. Start beeline

```
!connect jdbc:hive2://localhost:10000 root root
# su - hdfs
# hive password in /etc/hive/conf/hive-site.xml
# connect to database default with user root and password root
# beeline -u jdbc:hive2://localhost:10000/default root root
```

2. List databases

```
show databases;
```

3. Use database

```
use default;
```

4. Create parquet tables

```
create external table customer_parq(id string, fname string, lname string)
    STORED AS PARQUET location 's3a://customer-data-parq/customer.parq';
insert into customer_parq select * from customer_text;
```

5. List tables

```
show tables;
```

6. Add text to table

```
load data inpath 's3a://customer-data-text/customer.csv' overwrite into table customer_text;
```

7. Create a table on hdfs

```
create table customer_text2 (id string, fname string, lname string)
row format delimited fields terminated by '\t'
lines terminated by '\n' stored as textfile;
insert into customer_text2 select * from customer_text
```

8. Exit beeline cli

```
!exit
```

### Setup minio client - mc 

1. Setup minio clound storage

```
mc config host add minio http://localhost:9000 V42FCGRVMK24JJ8DHUYG bKhWxVF3kQoLY9kFmt91l+tDrEoZjqnWXzY9Eza
```

2. List buckets

```
mc ls minio
```

3. List files in buckets

```
mc ls minio/airline-parq
```

4. Make bucket to store new table

```
mc mb minio/airline-parq2
```

4. Start the Presto cli

```
java -jar ~/presto-cli.jar --server localhost:8080 --catalog minio --schema default
```

5. Show tables;

```
show tables;
```

6. Create a parquet table using Presto

```
create table airline_parq (
    Year              SMALLINT,
    Month             TINYINT,
    DayofMonth        SMALLINT,
    DayOfWeek         TINYINT,
    DepTime           SMALLINT,
    CRSDepTime        SMALLINT,
    ArrTime           SMALLINT,
    CRSArrTime        SMALLINT,
    UniqueCarrier     VARCHAR,
    FlightNum         INTEGER,
    TailNum           VARCHAR,
    ActualElapsedTime INTEGER,
    CRSElapsedTime    INTEGER,
    AirTime           INTEGER,
    ArrDelay          INTEGER,
    DepDelay          INTEGER,
    Origin            VARCHAR,
    Dest              VARCHAR,
    Distance          INTEGER,
    TaxiIn            INTEGER,
    TaxiOut           INTEGER,
    Cancelled         TINYINT,
    CancellationCode  VARCHAR,
    Diverted          TINYINT,
    CarrierDelay      INTEGER,
    WeatherDelay      INTEGER,
    NASDelay          INTEGER,
    SecurityDelay     INTEGER,
    LateAircraftDelay INTEGER
)
with (
    format = 'PARQUET',
    external_location = 's3a://airline-parq/'
);
```

7. Query data

```
select Year, Month, DayofMonth from airline_parq limit 5;
```

8. Create new parquet file with subset

```
create table airline_parq2 (
    Year              SMALLINT,
    Month             TINYINT,
    DayofMonth        SMALLINT,
    DayOfWeek         TINYINT,
    DepTime           SMALLINT,
    CRSDepTime        SMALLINT,
    ArrTime           SMALLINT,
    CRSArrTime        SMALLINT,
    UniqueCarrier     VARCHAR,
    FlightNum         INTEGER,
    TailNum           VARCHAR,
    ActualElapsedTime INTEGER,
    CRSElapsedTime    INTEGER,
    AirTime           INTEGER,
    ArrDelay          INTEGER,
    DepDelay          INTEGER,
    Origin            VARCHAR,
    Dest              VARCHAR,
    Distance          INTEGER,
    TaxiIn            INTEGER,
    TaxiOut           INTEGER,
    Cancelled         TINYINT,
    CancellationCode  VARCHAR,
    Diverted          TINYINT
)
with (
    format = 'PARQUET'
);

insert into airline_parq2
select 
    Year,
    Month,
    DayofMonth,
    DayOfWeek,
    DepTime,
    CRSDepTime,
    ArrTime,
    CRSArrTime,
    UniqueCarrier,
    FlightNum,
    TailNum,
    ActualElapsedTime,
    CRSElapsedTime,
    AirTime,
    ArrDelay,
    DepDelay,
    Origin,
    Dest,
    Distance,
    TaxiIn,
    TaxiOut,
    Cancelled,
    CancellationCode,
    Diverted
from airline_parq
limit 1000;
```

### Create the environment manually

1. Create a Pipfile

```
pipenv --python $(which python3)
```

2. Setup the libraries manually

```
pipenv install ipython --skip-lock
pipenv install requests --skip-lock
pipenv install pandas --skip-lock
pipenv install mypy --skip-lock
pipenv install flake8 --skip-lock
pipenv install yapf --skip-lock
pipenv lock

# pipenv install fsspec              # needed for dask file-system operations
# pipenv install dask                # core dask only
# pipenv install "dask[array]"       # Install requirements for dask array
# pipenv install "dask[bag]"         # Install requirements for dask bag
# pipenv install "dask[dataframe]"   # Install requirements for dask dataframe
# pipenv install "dask[delayed]"     # Install requirements for dask delayed
# pipenv install "dask[distributed]" # Install requirements for distributed dask
# pipenv install awscli
# pipenv install s3fs
# pipenv install toolz  # for dask
# pipenv install graphviz  # for dask
# pipenv install pyarrow
```

3. Setup AWS configuration

```
export AWS_ACCESS_KEY_ID=V42FCGRVMK24JJ8DHUYG
export AWS_SECRET_ACCESS_KEY=bKhWxVF3kQoLY9kFmt91l+tDrEoZjqnWXzY9Eza
aws configure set default.s3.signature_version s3v4
aws --endpoint-url http://192.168.33.10:9000 s3 ls
```

4. Use Python to access the buckets

```
python3 python/s3-process.py
```

### Presto cli

1. Start the Presto cli

```
mc mb minio/customer-data-parq2
docker exec -it presto presto-cli
```

2. Show catalogs

```
show catalogs;
```

3. Show schemas

```
show schemas from minio;
```

4. Use a schema

```
use minio.default;
```

5. Show catalogs

```
show schemas;
```

6. Show tables;

```
show tables;
```

7. Create a parquet table using Presto

```
create table customer_parq2(id varchar, fname varchar, lname varchar)
with (
    format = 'PARQUET',
    external_location = 's3a://customer-data-parq2/'
);
insert into customer_parq2 select * from customer_parq;
```

## Alluxio

https://www.alluxio.io/blog/getting-started-with-the-alluxio-presto-sandbox/

docker-compose.yml
presto-ports:
- '8080:8080' - http://10.0.0.2:8080/ui/login.html
hadoop-ports:
- '10000:10000' - http://10.0.0.2:10000/
- '8020:8020'
- '9083:9083'
minio-ports:
- '9001:9001' - http://10.0.0.2:9001/

## Spark

### Get project

1. Change to the root directory
```
cd /vagrant
```

2. Clone the spark project
```
git clone https://github.com/big-data-europe/docker-spark
```

3. Change to the spark directory
```
cd docker-spark
```

4. Start Spark
```
docker-compose up -d
```

5. Start pyspark with Python 3

``` docker-compose exec spark-master bash -c "PYSPARK_PYTHON=python3
/spark/bin/pyspark"
```

## Miscellaneous

Setup up AWS cli

```
export AWS_CONFIG_FILE=$(realpath ./do_not_checkin/credentials-minio)
aws --endpoint=http://10.0.0.2:9001/ s3 ls
```

[Metaflow S3](https://docs.metaflow.org/metaflow/data#data-in-s-3-metaflow-s3)

[Hyperloglog and minhash](https://tech.nextroll.com/blog/data/2013/07/10/hll-minhash.html)

[Python datasketch](https://github.com/ekzhu/datasketch)

## Clickhouse

1. Connect to Clickhouse

```
clickhouse-client -h 10.0.0.2 --highlight 0
```

2. Select database

```
use default
```

3. List tables

```
show tables
```

4. Load parquet data into clickhouse

```
cat ft_million_clustered.parq | clickhouse-client -h 10.0.0.2 --query="INSERT INTO ft_million_clustered FORMAT Parquet"
```

5. Get the number of rows in the table

```
select count(*) from ft_million_clustered;
```

### Clickhouse local


```
echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"

cat ft_million_clustered.parq| clickhouse-local --structure "id Int64, grp_code Int64" \
   --input-format "Parquet" --query "SELECT avg(id), avg(grp_code) FROM table"

cat ft_million_clustered.parq| clickhouse-local --structure "id Int64, grp_code Int64" \
   --input-format "Parquet" --queries-file clickhouse-queries-file.txt

```

## Frank command line

To run the frank command line type the following

```
export PYTHONPATH=.:$PYTHONPATH
echo typer frank.py run hive/presto
```

## Hadoop Hive Presto Spark

These docker services are created from https://github.com/tech4242/docker-hadoop-hive-parquet.git

1. Install postgresql client

```
sudo apt install postgresql-client-12  # for Ubuntu 20.04
```

2. Connect to postgresql database

```
psql -h localhost -p 5432 -d metastore -U hive
```

### Start services

1. Start the services

```
docker-compose up -d
```

2. View the running services and port

``
docker-compose ps
``

### View the services using the browser

1. Connect to the server using ssh with SOCKS port forwarding (-D option)
2. View the datanode at http://<server-ip>:50075/
3. View the hive server at http://<server-ip>:10000/
4. View the minio (S3) server at http://<server-ip>:9001/
5. View the namenode at http://<server-ip>:50070/
6. View the Presto server at http://<server-ip>:8088/
7. View the Spark master at http://<server-ip>:8080/
8. View the Spark worker at http://<server-ip>:8081/

### HDFS quickstart

1. Go the bash shell on the name node
docker-compose exec namenode bash

2. List the directories on the hdfs system
hdfs dfs -ls /user/hive/warehouse

```
# not tested
Copy breweries.csv to the namenode.
docker cp breweries.csv namenode:breweries.csv

Create a HDFS directory /data//openbeer/breweries.
hdfs dfs -mkdir /data

Copy breweries.csv to HDFS:
hdfs dfs -put breweries.csv /data/breweries.csv
```

## Links

1. [Presto releases][1000]

[1000]: https://prestodb.github.io/docs/current/release.html

2. [Minio versions][1020]

[1020]: https://github.com/minio/minio/releases

3. [Data lake engine][1030]

[1030]: https://www.youtube.com/watch?v=yMCV9RA6kNQ

### Other software

* [Python driver for Presto from Dropbox][1120]

[1120]: https://github.com/dropbox/PyHive

* [Presto with Minio][1130]

[1130]: https://johs.me/posts/big-data-stack-running-sql-queries/

[Presto Minio Docker][1140]

[1140]: https://github.com/starburstdata/presto-minio

* [On premise AI with Presto and Minio][1150]

[1150]: https://blog.minio.io/building-an-on-premise-ml-ecosystem-with-minio-powered-by-presto-weka-r-and-s3select-feature-fefbbaa87054

* Minio [S3 Select][1160]

[1160]: https://docs.min.io/docs/minio-select-api-quickstart-guide.html

[Using DBT][1170]

[1170]: https://medium.com/the-telegraph-engineering/dbt-a-new-way-to-handle-data-transformation-at-the-telegraph-868ce3964eb4

[Integration architecture][1180]

[1180]: https://github.com/gschmutz/integration-architecture-workshop

Stanford course: [Mining massive datasets][1190]

[1190]: https://www.edx.org/course/mining-massive-datasets

### Presto videos

* Using [Presto with Jupyter][1300]

[1300]: https://www.youtube.com/watch?v=67gXN5697Vw

* [Presto optimization][1310]

[1310]: https://www.youtube.com/watch?v=Z2C-iYNm2wU

* [Data Engineering principles][1320]

[1320]: https://www.youtube.com/watch?v=pzfgbSfzhXg

### Miscellaneous

#### Java CLI and GUI tool to connect to databases via JDBC

To setup SqlWorkbench
WORKBENCH_JDK=C:\sw\java\jdk-13.0.1

Download Java from http://jdk.java.net/
Use JDK 13.0.1 and the zip file for Windows

Run `SQLWorkbench64.exe` to launch the GUI

Run `sqlwbconsole64.exe` to launch the command line interface

Run `WbHelp` for a list of commands

#### IP addresses on Presto

```sql
select length(from_hex(replace('2001:0db8:aaaa:bbbb:cccc:dddd:eeee:aaaa', ':', '')));  -- 16
select length(replace('2001:0db8:aaaa:bbbb:cccc:dddd:eeee:aaaa', ':', ''));  -- 32
select length('2001:0db8:aaaa:bbbb:cccc:dddd:eeee:aaaa');  -- 39
select CAST(from_hex(replace('2001:0db8:aaaa:bbbb:cccc:dddd:eeee:aaaa', ':', '')) AS IPADDRESS);  -- 2001:db8:aaaa:bbbb:cccc:dddd:eeee:aaaa
select CAST(from_hex(replace('0000:0000:0000:0000:0000:ffff:eeee:aaaa', ':', '')) AS IPADDRESS);  -- 238.238.170.170
select CAST(from_hex(replace('2001:0000:0000:0000:cccc:0000:0000:aaaa', ':', '')) AS IPADDRESS);  --  2001::cccc:0:0:aaaa
select CAST('2001:db8:0000:000:cccc:0000:0000:aaaa' AS IPADDRESS);  -- 2001:db8::cccc:0:0:aaaa
select from_hex('20010db8aaaabbbbccccddddeeeeaaaa');  -- 20 01 0d b8 aa aa bb bb cc cc dd dd ee ee aa aa
```

#### Presto query fails

```sql
-- Query 20191020_171021_00057_88qke failed: Cannot write to non-managed Hive table
CREATE TABLE ip_data2 ( name varchar, email varchar, city varchar, state varchar, date_time TIMESTAMP, randomdata BIGINT, ipv4 VARBINARY, ipv6 VARBINARY ) with (format='ORC', external_location='s3a://customer-data-orc/');
insert into ip_data2 values('name1', 'email@co.com', 'city1', 'state1', localtimestamp, 2344, from_hex('eeeeaaaa'), from_hex('20010db8aaaabbbbccccddddeeeeaaaa'));
```

#### Presto query succeeds

```sql
CREATE TABLE ip_data4 ( name varchar, email varchar, city varchar, state varchar, date_time TIMESTAMP, randomdata BIGINT, ipv4 VARBINARY, ipv6 VARBINARY );
insert into ip_data4 values('name1', 'email@co.com', 'city1', 'state1', localtimestamp, 2344, from_hex('eeeeaaaa'), from_hex('20010db8aaaabbbbccccddddeeeeaaaa'));
```

## Presto/Hive examples

### Presto versions

```
presto-minio
docker-hadoop-hive-parquet
docker-hadoop-spark-TO_DEL
```

#### Setup Hadoop Hive Presto

Use this repo: https://github.com/tech4242/docker-hadoop-hive-parquet.git

#### Setup Spark

Hive does not work correctly: https://github.com/Marcel-Jan/docker-hadoop-spark.git

### Presto notes

Create tables using the file ./presto-minio/presto-union-intersect.sql

Query Presto and Hive metadata using ./python/presto-hive.py

## Presto Minio Docker

https://github.com/njanakiev/presto-minio-docker

Docker images

* ahanaio/prestodb
* mariadb
* bitsondatadev/hive-metastore
* minio
* postgres

1. Change to the docker-compose directory
cd presto-minio-docker

2. Start the containers
docker-compose up -d

3. 
