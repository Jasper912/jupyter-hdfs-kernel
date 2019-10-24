#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/24

from hdfs_kernel.constants import LANG_HDFS
from hdfs_kernel.kernels.wrapperkernel.hdfsbase import HdfsKernelBase


class HdfsKernel(HdfsKernelBase):
    banner = 'Hdfs REPL'

    def __init__(self, **kwargs):
        implementation = 'Hdfs'
        implementation_version = '1.0'
        language = 'no-op'
        language_version = '0.1'
        language_info = {
            'name': 'hdfs',
            'mimetype': 'text/x-python',
            'codemirror_mode': {'name': 'python', 'version': 3},
            'pygments_lexer': 'python3'
        }


        super(HdfsKernel, self).__init__(implementation, implementation_version, language, language_version,
                                          language_info, **kwargs)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=HdfsKernel)
