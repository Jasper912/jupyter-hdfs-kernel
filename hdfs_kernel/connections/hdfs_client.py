#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/27

"""
    Hdfs Clients
"""

import hdfs_kernel.utils.configuration as config
from hdfs.ext.kerberos import KerberosClient as KerberosClientBase
import posixpath as psp

class HdfsKerberosClient(KerberosClientBase):

    def __init__(self, nameservice, **kwargs):
        urls = self._build_urls(nameservice)
        super(HdfsKerberosClient, self).__init__(urls, **kwargs)

    def _build_urls(self, nameservice, port=50070):
        ha_urls = []
        nodes = config.web_hdfs_nodes().get(nameservice)
        assert nodes, "Hdfs nodes not found"
        for node in nodes:
            url = "http://{}:{}".format(node, port)
            ha_urls.append(url)

        return ";".join(ha_urls)

    def resolve(self, hdfs_path):
        """
            some path start with "resolved", so overwrite it
        """
        return psp.normpath(hdfs_path)
