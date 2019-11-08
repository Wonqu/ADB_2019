Install requirements (recommended to do it in [virtualenv](https://virtualenv.pypa.io/en/latest/) or using [pew](https://github.com/berdario/pew))
```
pip install -r requirements.txt
```

Run docker container:
```
docker-compose up
```

Create tables in database (not necessary if you have the dump file):
```
python models.py
```

Create data (not necessary if you have the dump file):
```
python factories.py
```

Restore data from dump:
```
docker exec -it adb_2019_db_1 sh
psql -U postgres -d postgres -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
psql postgres -U postgres < /var/lib/postgresql/dumps/dump.sql
```