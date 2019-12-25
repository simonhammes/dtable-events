# -*- coding: utf-8 -*-
import configparser
import logging
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.event import contains, listen
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool

logger = logging.getLogger(__name__)
Base = declarative_base()  # base class of model classes in events.models


def create_engine_from_conf(config):
    need_connection_pool_fix = True

    backend = config.get('DATABASE', 'type')

    if backend == 'mysql':
        if config.has_option('DATABASE', 'host'):
            host = config.get('DATABASE', 'host').lower()
        else:
            host = 'localhost'

        if config.has_option('DATABASE', 'port'):
            port = config.getint('DATABASE', 'port')
        else:
            port = 3306

        username = config.get('DATABASE', 'username')
        password = config.get('DATABASE', 'password')
        db_name = config.get('DATABASE', 'db_name')

        db_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % \
                 (username, quote_plus(password), host, port, db_name)
        logger.debug('[dtable_events] database: mysql, name: %s', db_name)
    else:
        logger.error("Unknown database backend: %s" % backend)
        raise RuntimeError("Unknown database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed
    # by mysql daemon if idle for too long.
    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    if need_connection_pool_fix and not contains(Pool, 'checkout', ping_connection):
        # We use contains to double check in case we call
        # create_engine multiple times in the same process.
        listen(Pool, 'checkout', ping_connection)

    return engine


def init_db_session_class(config):
    """Configure session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Init db session class error: %s" % e)
        raise RuntimeError("Init db session class error: %s" % e)

    session = sessionmaker(bind=engine)
    return session


def create_db_tables(config):
    # create events tables if not exists.
    try:
        engine = create_engine_from_conf(config)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Create tables error: %s" % e)
        raise RuntimeError("Create tables error: %s" % e)

    Base.metadata.create_all(engine)


# This is used to fix the problem of "MySQL has gone away" that happens when
# mysql server is restarted or the pooled connections are closed by the mysql
# server because being idle for too long.

# See http://stackoverflow.com/a/17791117/1467959
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.close()
    except:
        logger.info('fail to ping database server, disposing all cached connections')
        connection_proxy._pool.dispose()

        # Raise DisconnectionError so the pool would create a new connection
        raise DisconnectionError()
