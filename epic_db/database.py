from contextlib import contextmanager

import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from epic_db.models import Base


_instances = {}


class Config(object):

    def __new__(cls, *args, **kw):
        # singleton implementation
        if not (cls in _instances):
            instance = super(Config, cls).__new__(cls)
            _instances[cls] = instance

        return _instances[cls]

    def __init__(self, engine=None):
        self.engine = engine
        self._sessionmaker = None

    @property
    def engine(self):
        if self._engine is None:
            self.create_engine('sqlite://')
        return self._engine

    @engine.setter
    def engine(self, val):
        self._engine = val
        if val is not None:
            self._sessionmaker = sessionmaker(bind=self.engine)
        else:
            self._sessionmaker = None

    @property
    def Session(self):
        return self._sessionmaker

    def create_engine(self, conn_str, *arg, **kwargs):
        if 'sqlite' in conn_str:
            engine = sql.create_engine(
                conn_str, connect_args={'check_same_thread': False},
                poolclass=StaticPool)

        else:
            engine = sql.create_engine(
                conn_str, pool_recycle=300, pool_size=3, max_overflow=10,
                pool_timeout=120)
        self.engine = engine


config = Config()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = config.Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_db():
    """create sqlite database from models schema"""
    Base.metadata.create_all(config.engine)  # doesn't create if exists
    return True


def delete_db():
    """create sqlite database from models schema"""
    Base.metadata.drop_all(config.engine)  # doesn't create if exists
    return True
