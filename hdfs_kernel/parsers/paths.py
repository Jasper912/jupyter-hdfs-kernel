#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/30

import re
from hdfs_kernel.constants import HDFS_PREFIX, RESOLVED_PREFIX
import hdfs_kernel.utils.configuration as config


def is_hdfs_path(path):
    is_correct_prefix = any([
        path.startswith(HDFS_PREFIX),
        path.startswith(RESOLVED_PREFIX),
        path.startswith("/")
    ])
    if not is_correct_prefix:
        return False

    return True

def is_local_path(path):
    pattern = r"([./|/][a-zA-Z\./]*[\s]?)"
    return re.findall(pattern, path)


class HdfsPath(dict):
    """
        hdfs path parser
        resolve name service and real path
        eg: "resolved://nfjd-prod-ns2/user/hive/", "hdfs://nfjd-prod-ns3/user/"
    """

    def __init__(self, path):
        self.path = path
        self.protocols = [HDFS_PREFIX, RESOLVED_PREFIX]
        self.item = self.resolve()

        super(HdfsPath, self).__init__(self.item)

    def resolve(self):
        nameservice = self.get_nameservice()
        real_path = self.get_real_path(nameservice=nameservice) # /user/hive
        path_service = self.path.replace(real_path, "") # hdfs://nfjd-prod-ns3
        struct = {
            "source_path": self.path,
            "nameservice": nameservice,
            "path": real_path,
            "path_service": path_service
        }
        return struct

    def get_nameservice(self):
        nameservice_list = config.web_hdfs_name_services()
        pattern = "|".join(nameservice_list)
        match = re.findall(pattern, self.path, flags=1)
        if match:
            return match.pop()

        return config.default_name_service()

    def get_real_path(self, nameservice=None):
        replace_times = 1
        path = self.strip_protocols_prefix(self.path)
        if nameservice:
            if path.startswith(nameservice):
                path = path.replace(nameservice, "", replace_times)

        return path

    def strip_protocols_prefix(self, path):
        replace_times = 1
        for protocol in self.protocols:
            if path.startswith(protocol):
                path = path.replace(protocol, "", replace_times)

        return path
