Install requirements (recommended to do it in [virtualenv](https://virtualenv.pypa.io/en/latest/) or using [pew](https://github.com/berdario/pew))
```
pip install -r requirements.txt
```

Run docker container:
```
docker-compose up
```

Create tables in database:
```
python models.py
```

Create data:
```
python factories.py
```