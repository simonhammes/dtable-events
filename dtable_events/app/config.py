# -*- coding: utf-8 -*-
import logging
import configparser

logger = logging.getLogger(__name__)
global_conf = object
global_dtable_server_conf = object


def get_config(config_file, dtable_server_conf):
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        global global_conf
        global global_dtable_server_conf
        global_conf = config
        global_dtable_server_conf = dtable_server_conf
    except Exception as e:
        logger.critical("Failed to read config file %s: %s" % (config_file, e))
        raise RuntimeError("Failed to read config file %s: %s" % (config_file, e))

    return config


def is_syslog_enabled(config):
    if config.has_option('Syslog', 'enabled'):
        try:
            return config.getboolean('Syslog', 'enabled')
        except ValueError:
            return False

    return False
