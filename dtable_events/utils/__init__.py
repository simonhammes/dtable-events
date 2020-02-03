# -*- coding: utf-8 -*-
import os
import sys
import logging
import configparser
import subprocess

logger = logging.getLogger(__name__)
pyexec = None


def find_in_path(prog):
    if 'win32' in sys.platform:
        sep = ';'
    else:
        sep = ':'

    dirs = os.environ['PATH'].split(sep)
    for d in dirs:
        d = d.strip()
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None


def parse_bool(v):
    if isinstance(v, bool):
        return v

    v = str(v).lower()

    if v == '1' or v == 'true':
        return True
    else:
        return False


def parse_interval(interval, default):
    if isinstance(interval, (int, int)):
        return interval

    interval = interval.lower()

    unit = 1
    if interval.endswith('s'):
        pass
    elif interval.endswith('m'):
        unit *= 60
    elif interval.endswith('h'):
        unit *= 60 * 60
    elif interval.endswith('d'):
        unit *= 60 * 60 * 24
    else:
        pass

    val = int(interval.rstrip('smhd')) * unit
    if val < 10:
        logger.warning('insane interval %s', val)
        return default
    else:
        return val


def get_opt_from_conf_or_env(config, section, key, env_key=None, default=None):
    # Get option value from events.conf.
    # If not specified in events.conf, check the environment variable.
    try:
        return config.get(section, key)
    except configparser.NoOptionError:
        if env_key is None:
            return default
        else:
            return os.environ.get(env_key.upper(), default)


def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
        'python3.7',
        'python37',
        'python3.6',
        'python36',
    ]

    for prog in try_list:
        path = find_in_path(prog)
        if path is not None:
            return path

    path = os.environ.get('PYTHON', 'python')

    return path


def get_python_executable():
    # Find a suitable python executable
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec


def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    def quote(args):
        return ' '.join(['"%s"' % arg for arg in args])

    cmdline = quote(argv)
    if cwd:
        logger.debug('Running command: %s, cwd = %s', cmdline, cwd)
    else:
        logger.debug('Running command: %s', cmdline)

    with open(os.devnull, 'w') as devnull:
        kwargs = dict(cwd=cwd, env=env, shell=True)

        if suppress_stdout:
            kwargs['stdout'] = devnull
        if suppress_stderr:
            kwargs['stderr'] = devnull

        if output:
            kwargs['stdout'] = output
            kwargs['stderr'] = output

        return subprocess.Popen(cmdline, **kwargs)


def run_and_wait(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    proc = run(argv, cwd, env, suppress_stdout, suppress_stderr, output)
    return proc.wait()
