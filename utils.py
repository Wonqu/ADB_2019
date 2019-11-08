from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from config import local_env

engine = create_engine('postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{POSTGRES_PORT}/{POSTGRES_DB}'.format(**local_env))
session = scoped_session(sessionmaker(bind=engine))

