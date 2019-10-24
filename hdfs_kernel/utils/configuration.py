#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/27

import getpass
import copy
import sys
import base64
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME
from hdijupyterutils.utils import join_paths
from hdijupyterutils.configuration import override as _override
from hdijupyterutils.configuration import override_all as _override_all
from hdijupyterutils.configuration import with_override

from hdfs_kernel.constants import HOME_PATH, CONFIG_FILE
from hdfs_kernel.constants import HDFS_LOGGER_NAME

d = {}
path = join_paths(HOME_PATH, CONFIG_FILE)


def override(config, value):
    _override(d, path, config, value)


def override_all(obj):
    _override_all(d, obj)


_with_override = with_override(d, path)


# Helpers


# Configs
@_with_override
def web_hdfs_nodes():
    return {}

@_with_override
def web_hdfs_name_services():
    nodes = web_hdfs_nodes()
    return nodes.keys()

@_with_override
def default_name_service():
    return None

@_with_override
def session_configs():
    return {}

@_with_override
def user_workspace():
    username = getpass.getuser()
    workspace = f"/home/{username}/workspace"
    return workspace


@_with_override
def logging_config():
    return {
        "version": 1,
        "formatters": {
            "hdfsFormatter": {
                "format": "%(asctime)s\t%(levelname)s\t%(message)s",
                "datefmt": ""
            }
        },
        "handlers": {
            "hdfsHandler": {
                "class": LOGGING_CONFIG_CLASS_NAME,
                "formatter": "hdfsFormatter",
                "home_path": HOME_PATH
            }
        },
        "loggers": {
            HDFS_LOGGER_NAME: {
                "handlers": ["hdfsHandler"],
                "level": "WARNING",
                "propagate": 0
            }
        }
    }


@_with_override
def fatal_error_suggestion():
    return u"""The code failed because of a fatal error:
\t{}.

Some things to try:
a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
c) Restart the kernel."""


@_with_override
def resource_limit_mitigation_suggestion():
    return ""


@_with_override
def ignore_ssl_errors():
    return False


@_with_override
def coerce_dataframe():
    return True

@_with_override
def default_maxrows():
    return 1000

@_with_override
def default_samplefraction():
    return 0.1


@_with_override
def heartbeat_refresh_seconds():
    return 30


@_with_override
def heartbeat_retry_seconds():
    return 10


@_with_override
def custom_headers():
    return {}


@_with_override
def retry_seconds_to_sleep_list():
    return [0.2, 0.5, 1, 3, 5]
