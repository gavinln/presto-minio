# presto-minio

* Source code - [Github][1]
* Author - Gavin Noronha - <gavinln@hotmail.com>

[1]: https://github.com/gavinln/presto-minio

## About

This project provides a [Ubuntu (18.04)][10] [Vagrant][20] Virtual Machine (VM)
with multiple big data projects. It uses Docker containers to setup
[Hadoop][30], [Hive][40], [Presto][50], [Minio][60], [Superset][70],
[Hue][80] 

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

## Setup Presto, Mino and Hive

1. Clone the project

```
git clone https://github.com/starburstdata/presto-minio
```

2. Change to the project directory

```
cd presto-mino
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

6. Follow instructions at https://github.com/starburstdata/presto-minio

7. Try intake: https://intake.readthedocs.io/en/latest/index.html

## Links

### Presto releases

https://prestodb.github.io/docs/current/release.html

### Superset releases

https://github.com/apache/incubator-superset/releases

### Minio versions

https://github.com/minio/minio/releases


### Other software

[Python driver for Presto from Dropbox][1020]

[1020]: https://github.com/dropbox/PyHive

[Presto with Minio][1030]

[1030]: https://johs.me/posts/big-data-stack-running-sql-queries/

[Presto Minio Docker][1040]

[1040]: https://github.com/starburstdata/presto-minio

[On premise AI with Presto and Minio][1050]

[1050]: https://blog.minio.io/building-an-on-premise-ml-ecosystem-with-minio-powered-by-presto-weka-r-and-s3select-feature-fefbbaa87054

Command line client for multiple databases: [usql][1060]

[1060]: https://github.com/xo/usql

### Miscellaneous

On Presto

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
