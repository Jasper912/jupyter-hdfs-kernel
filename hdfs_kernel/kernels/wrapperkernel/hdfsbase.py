#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/24

from ipykernel.kernelbase import Kernel
from hdijupyterutils.ipythondisplay import IpythonDisplay
from hdfs_kernel.parsers.code_parser import HdfsCodeParser
from hdfs_kernel.connections.manager import HdfsSessionManager
import getpass
from hdfs_kernel.command import CommandDispatcher, CommandResult
from hdfs_kernel.exceptions import handle_expected_exceptions, wrap_unexpected_exceptions
import traceback
from pandas import DataFrame
from hdfs_kernel.constants import HELP_TIPS

class HdfsKernelBase(Kernel):

    def __init__(self, implementation, implementation_version, language,
                 language_version, language_info, **kwargs):
        self.implementation = implementation
        self.implementation_version = implementation_version
        self.language = language
        self.language_version = language_version
        self.language_info = language_info


        super(HdfsKernelBase, self).__init__(**kwargs)

        self._fatal_error = None
        self.ipython_display = IpythonDisplay()
        self.session_manager = HdfsSessionManager()

    @wrap_unexpected_exceptions
    @handle_expected_exceptions
    def do_execute(self, code, silent, store_history=True,
                   user_expressions=None, allow_stdin=False):

        try:
            parse_result = HdfsCodeParser(code).parse()
            if parse_result['error']:
                self.send_error(parse_result['error'])
                return self.finish()

            command = parse_result.get("command")
            if command == "-help":
                self.send_info(HELP_TIPS)
                return self.finish()

            result = self.execute_hdfs_command(parse_result)
            assert isinstance(result, CommandResult), \
                "Wrong type of command execute result, should be isinstance of  CommandResult "
            self.send_result(result)
            return self.finish()
        except Exception as e:
            traceback.print_exc()
            print("%r" % e)
            raise e

    def execute_hdfs_command(self, command_settings):
        command = command_settings.get("command")
        args = command_settings.get("args")
        options = command_settings.get("options")
        return CommandDispatcher(command, self.session_manager).execute(*args, **options)

    def df_to_html(self, df):
        return df.fillna('NULL').astype(str).to_html(notebook=True, index=False)

    def finish(self):
        msg = {
            'status': 'ok',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {}
        }
        return msg

    def send_error(self, contents):
        self.send_response(self.iopub_socket, 'stream', {
            'name': 'stderr',
            'text': str(contents)
        })
        return {
            'status': 'error',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {}
        }

    def send_info(self, contents):
        self.send_response(self.iopub_socket, 'stream', {
            'name': 'stdout',
            'text': str(contents)
        })

    def send_result(self, result):
        if not result['status']:
            # Execute failed
            return self.send_error(result['message'])

        if isinstance(result['data'], DataFrame):
            response = self.df_to_html(result['data'])
        else:
            response = result['data']

        self.send_response(
            self.iopub_socket,
            'execute_result', {
                "execution_count": self.execution_count,
                'data': {
                    "text/html": response,
                },
                "metadata": {
                    "image/png": {
                        "width": 640,
                        "height": 480,
                    },
                }
            }
        )
