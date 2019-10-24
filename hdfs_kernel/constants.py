#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/24
import os

LANG_HDFS = "hdfs"

__version__ = 1.0
KERNEL_NAME = "Hdfs"
DISPLAY_NAME = "HDFS"
LANGUAGE = 'hdfs'
DEFAULT_TEXT_LANG = ['en']

HOME_PATH = os.environ.get("HDFS_CONF_DIR", "~/.hdfs")
CONFIG_FILE = os.environ.get("HDFS_CONF_FILE", "config.json")


HDFS_LOGGER_NAME = "HdfsLogger"

INTERNAL_ERROR_MSG = "An internal error was encountered.\n" \
                     "Error:\n{}"
EXPECTED_ERROR_MSG = "An error was encountered:\n{}"

HDFS_PREFIX = "hdfs://"
RESOLVED_PREFIX = "resolved://"

HDFS_FILE_TYPE = "FILE"
HDFS_DIRECTORY_TYPE = "DIRECTORY"

HELP_TIPS = """
Usage: hadoop fs [generic options]
	[-chgrp GROUP PATH...]
	[-chmod <MODE[,MODE]... | OCTALMODE> PATH...]
	[-chown [OWNER][:[GROUP]] PATH...]
	[-copyFromLocal [-f] [-p] [-l] <localsrc> ... <dst>]
	[-count [-q] [-h] [-v] <path> ...]
	[-cp [-f] [-p | -p[topax]] <src> ... <dst>]
	[-du [-s] [-h] <path> ...]
	[-get [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
	[-help]
	[-ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [<path> ...]]
	[-mkdir [-p] <path> ...]
	[-mv <src> ... <dst>]
	[-put [-f] [-p] [-l] <localsrc> ... <dst>]
	[-rm [-f] [-r|-R] <src> ...]
"""
