# presto-minio

* Source code - [Github][1]
* Author - Gavin Noronha - <gavinln@hotmail.com>

[1]: 

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

6. Clone the project with the Docker compose file

```
git clone https://github.com/johannestang/bigdata_stack
```

7. Change to the project to create Docker containers

```
cd bigdata_stack/
```

8. Setup the environment

```
cp sample.env .env
set -o allexport
source .env
set +o allexport
```

9. Create the containers

```
docker-compose up -d
```

## Setup the software

1. Follow the instructions [here][200]

[200]: https://johs.me/posts/big-data-stack-running-sql-queries/

## Links

[Python driver for Presto from Dropbox][1020]

[1020]: https://github.com/dropbox/PyHive

[Presto with Minio][1030]

[1030]: https://johs.me/posts/big-data-stack-running-sql-queries/

[Presto Minio Docker][1040]

[1040]: https://github.com/starburstdata/presto-minio

[On premise AI with Presto and Minio][1050]

[1050]: https://blog.minio.io/building-an-on-premise-ml-ecosystem-with-minio-powered-by-presto-weka-r-and-s3select-feature-fefbbaa87054
