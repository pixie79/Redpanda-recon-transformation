
Load docker schemas into registry

``` bash
docker exec -it schemas /venv/bin/python3.9 python/importer.py
```

Load the recon schema
```bash
go/transform/register.sh
```

Enter the Flink SQL Shell
```bash
docker exec -it jobmanager ./bin/sql-client.sh
```

create 