# Run PrestoDB

Presto is available in two different versions.

* [PrestoDB][100]
* [PrestoSQL][110]

[100]: https://prestodb.io/
[110]: https://prestosql.io/

## PrestoDB

PrestoDB uses a version numbering starting with "0.", for example 0.221

PrestoDB images is available in this [respository][120] of Docker files.

[120]: https://github.com/vimagick/dockerfiles

### Install

1. Start the Docker daemon

```
sudo service docker start
```

2. Get the PrestoDB image

[Ahana][130] releases PrestoDB docker images.

[130]: https://ahana.io/getting-started/

Instructions for [PrestoDB Docker][140] images

[140]: https://hub.docker.com/r/ahanaio/prestodb-sandbox

```
docker pull ahanaio/prestodb-sandbox
docker exec -it presto presto-cli
```

. Start the service
docker-compose up -d

## PrestoSQL

PrestoSQL uses a release version number with three digits (no leading zero),
for example 340




