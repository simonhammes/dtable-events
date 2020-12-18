# -*- coding: utf-8 -*-
import logging
import configparser

logger = logging.getLogger(__name__)


def get_config(config_file):
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
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


class TaskMode(object):
    enable_foreground_tasks = False
    enable_background_tasks = False


def get_task_mode(task_mode_str):
    task_mode = TaskMode()
    if task_mode_str == 'foreground':
        task_mode.enable_foreground_tasks = True
    elif task_mode_str == 'background':
        task_mode.enable_background_tasks = True
    else:
        task_mode.enable_foreground_tasks = True
        task_mode.enable_background_tasks = True

    return task_mode
