#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/27

from hdijupyterutils.log import Log

import hdfs_kernel.utils.configuration as config
from hdfs_kernel.constants import HDFS_LOGGER_NAME


class HdfsLog(Log):
    def __init__(self, class_name):
        super(HdfsLog, self).__init__(HDFS_LOGGER_NAME, config.logging_config(), class_name)
