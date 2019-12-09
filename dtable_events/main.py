# -*- coding: utf-8 -*-
import os
import argparse
import logging

from dtable_events.config import get_config
from dtable_events.db import create_db_tables
from dtable_events.app import App


def main(conf):
    try:
        create_db_tables(conf)
    except Exception as e:
        logging.error(e)
        raise RuntimeError('Failed create tables.')

    app = App(config)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', help='log level')

    args = parser.parse_args()
    os.environ['DTABLE_EVENTS_CONFIG_FILE'] = os.path.expanduser(args.config_file)

    config = get_config(args.config_file)
    main(config)
