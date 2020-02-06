# presto-minio

* Source code - [Github][1]
* Author - Gavin Noronha - <gavinln@hotmail.com>

[1]: https://github.com/gavinln/presto-minio

## About

This project provides a [Ubuntu (18.04)][10] [Vagrant][20] Virtual Machine (VM)
with multiple big data projects. It uses Docker containers to setup
[Hadoop][30], [Hive][40], [Presto][50], [Minio][60], [Superset][70],
[Hue][80]. It also use Database tool (DBT) to transform data using a ELT process.

[10]: http://releases.ubuntu.com/18.04/
[20]: http://www.vagrantup.com/
[30]: https://hadoop.apache.org/
[40]: https://github.com/apache/hive
[50]: https://prestodb.github.io/
[60]: https://min.io/
[70]: https://github.com/apache/incubator-superset
[80]: https://github.com/cloudera/hue

There are [Ansible][90] scripts that automatically install the software when
the VM is started.

[90]: https://www.ansible.com/

## Setup the machine

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

5. Create a directory of Minio object storage (like S3)

    ```
    sudo mkdir /data
    sudo chown vagrant:vagrant /data
    ```

## Run Postgres

1. Change to the Postgres directory

```
cd /vagrant/postgres
```

2. Start the Docker containers

```
docker-compose up -d
```

3. Connect to the postgres database

```
psql -h localhost -U postgres
```

4. List databases

```
\l
```

5. Create the database dbt_example;

```
create database dbt_example;
```

3. Connect to adminer at http://192.168.33.10:8080

4. Enter the following options

```
System: PostgresSQL
Server: db
Username: postgres
Password: example
Database: postgres
```

5. Run the "SQL command"

```
create database dbt_example;
```

6. Change to dbt directory

```
cd /vagrant/python/postgres-dbt_example
```

7. Start the dbt project

```
dbt init postgres-dbt_example
```

8. Add to the ~/.dbt/profiles.yml

```
dbt_example:
  target: dev
  outputs:
    dev:
      type: postgres
      database: dbt_example
      user: postgres
      pass: example
      host: localhost
      port: 5432
      schema: default
      threads: 1
```

9. Test connection to the dbt_example database

```
psql -h localhost -U postgres -d dbt_example
```

10. Change the profile option in dbt_project.yml to dbt_example

11. List the resources of the project

```
dbt list
```

## Jaffle shop project

1. Change to the python code directory

```
cd /vagrant/python
```

2. Clone the Jaffle shop project

```
git clone https://github.com/fishtown-analytics/jaffle_shop
```

3. Setup ~/.dbt/profiles.yml with the following settings.

```
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: postgres
      database: dbt_example
      user: postgres
      pass: example
      host: localhost
      port: 5432
      schema: default
      threads: 1
```

4. Setup the Jaffle shop project

https://github.com/fishtown-analytics/jaffle_shop

## Run superset, hue, presto, hive using Docker

1. Clone the project with the Docker compose file

```
git clone https://github.com/johannestang/bigdata_stack
```

2. Change to the project to create Docker containers

```
cd bigdata_stack/
```

3. Setup the environment

```
cp sample.env .env
set -o allexport
source .env
set +o allexport
```

4. Create the containers

```
docker-compose up -d
```

5. Set username/password for Superset

```
./scripts/init-superset.sh
```

6. Initialize Hue

```
./scripts/init-hue.sh
```

7. To take down the entire software stack

```
docker-compose down
```

### Setup the software

1. Start the Beeline command line interface

```
./scripts/beeline.sh
```

2. Create a table in Hive

```
CREATE TABLE pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
SELECT * FROM pokes LIMIT 1;
!quit
```

3. View the dataset in the HDFS volume

```
http://192.168.33.10:50070/explorer.html#/user/hive/warehouse
```

4. Start the Presto CLI

```
./scripts/presto-cli.sh
```

5. Query the table just created

```
SELECT * FROM pokes LIMIT 1;
```

6. Quit the Presto CLI

```
quit
```

7. Setup data in S3

```
sudo mkdir -p /data/minio/data/datasets/iris
cd /data/minio/data/datasets/iris
sudo wget https://raw.githubusercontent.com/uiuc-cse/data-fa14/gh-pages/data/iris.csv
cd -
```

8. View the data in the object stores (similar to S3) at http://192.168.33.10:9000

9. Start Beeline to create a table based on data in S3

```
CREATE EXTERNAL TABLE iris (sepal_length DECIMAL, sepal_width DECIMAL, 
petal_length DECIMAL, petal_width DECIMAL, species STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3a://datasets/iris/'
TBLPROPERTIES ("skip.header.line.count"="1");
!quit
```

10. Start the Presto CLI

```
./scripts/presto-cli.sh
```

11. Query data from the iris table on S3 using Preso

```
SELECT * FROM iris LIMIT 1;
quit
```

12. Follow the instructions [here][200]

[200]: https://johs.me/posts/big-data-stack-running-sql-queries/

Access the servers here

    * Hadoop namenode: http://192.168.33.10:50070
    * Minio: http://192.168.33.10:9000
    * Presto: http://192.168.33.10:8080
    * Superset: http://192.168.33.10:8088
    * Hue: http://192.168.33.10:8888

## Setup Presto, Minio and Hive

1. Clone the project

```
git clone https://github.com/starburstdata/presto-minio
```

2. Change to the project directory

```
cd /vagrant/presto-minio
```

3. Add to file /vagrant/presto-minio/presto/minio.properties

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

4. View Mino at http://192.168.33.10:9000/

Use the access key and secret access key from the docker-compose.yml file
There are two folders called customer-data-text and customer-data-json

Create a new folder called customer-data-orc

5. View the Presto WebUI at http://192.168.33.10:8080/

6. Connect to the Hadoop master

```
docker exec -it hadoop-master /bin/bash
```

7. Start hive

```
su - hdfs
hive
```

8. Create the customer_text table

```
use default;
create external table customer_text(id string, fname string, lname string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3a://customer-data-text/';
select * from customer_text;
```

9. Exit the hive cli

```
exit;
```

10. Exit the Docker container

```
exit
```

10. Connect to Presto

```
docker exec -it presto presto-cli
```

11. Query data from Presto

```
use minio.default;
show tables;
select * from customer_text;
```

12. Exit from the Presto cli

```
quit
```

13. Follow instructions at https://github.com/starburstdata/presto-minio

14. Try intake: https://intake.readthedocs.io/en/latest/index.html

## DBT

1. Change to the dbt project root

    ```
    cd /vagrant/python
    ```

2. Create a project

    ```
    dbt init presto-customer
    ```

2. Create a ~/.dbt/profiles.yml file

    ```
    presto-customer:
      target: dev
      outputs:
        dev:
          type: presto
          method: none
          database: minio
          host: localhost
          port: 8080
          schema: default
          threads: 1
    ```

## Links

1. [Presto releases][1000]

[1000]: https://prestodb.github.io/docs/current/release.html

2. [Superset releases][1010]

[1010]: https://github.com/apache/incubator-superset/releases

3. [Minio versions][1020]

[1020]: https://github.com/minio/minio/releases

### Other software

* [Python driver for Presto from Dropbox][1120]

[1120]: https://github.com/dropbox/PyHive

* [Presto with Minio][1130]

[1130]: https://johs.me/posts/big-data-stack-running-sql-queries/

[Presto Minio Docker][1140]

[1140]: https://github.com/starburstdata/presto-minio

* [On premise AI with Presto and Minio][1150]

[1150]: https://blog.minio.io/building-an-on-premise-ml-ecosystem-with-minio-powered-by-presto-weka-r-and-s3select-feature-fefbbaa87054

Command line client for multiple databases: [usql][1160]

[1160]: https://github.com/xo/usql

### Miscellaneous

Other tools

#### usql go language CLI tool

Use usql to connect to Presto

```
usql presto://192.168.33.10:8080/minio/default
```

Execute query from the command line and get the results

```
usql presto://192.168.33.10:8080/minio/default -c "select * from customer_orc;"
```

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
