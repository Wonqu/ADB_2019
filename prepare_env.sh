#!/usr/bin/env bash

export LDFLAGS='-L/usr/local/lib -L/usr/local/opt/openssl/lib -L/usr/local/opt/readline/lib'
./venv/bin/pip install factory-boy==2.12.0
./venv/bin/pip install Faker==2.0.3
./venv/bin/pip install psycopg2==2.8.3
./venv/bin/pip install SQLAlchemy==1.3.9
./venv/bin/python models.py