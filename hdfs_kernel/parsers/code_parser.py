#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/25

import re
from .opt_parser import OptionParserProxy
from hdfs_kernel.exceptions import CommandNotAllowedException


class CodeParser(object):
    """
        parse execute code
    """

    def __init__(self, code):
        self.code = self._strip_multi_blank(code)

    def parse(self, code):
        raise NotImplemented

    def _strip_multi_blank(self, code):
        pattern = r"\s+"
        _code = re.sub(pattern, " ", code.strip())
        return _code


class HdfsCodeParser(CodeParser):
    """
        Parse hdfs commands
        only allow "hdfs dfs" or "hadoop fs"
    """
    allow_commands = [
        ("hdfs", "dfs"),
        ("hadoop", "fs")
    ]

    allow_sub_commands = (
        "-ls", "-du", "-get", "-put", "-copyFromLocal", "-help",
        "-cp", "-mv", "-mkdir", "-rm", "-chmod", "-chown", "-chgrp",
        "-count"
    )

    def parse(self):
        command_list = self.code.split(" ")
        self._validate(command_list)
        sub_command = command_list[2]
        options_list = command_list[3:]

        parser_result = OptionParserProxy(sub_command, options_list).parse()
        return parser_result

    def _validate(self, command_list):
        command = tuple(command_list[:2])
        if command not in self.allow_commands:
            self._raise_command_exception()

        sub_command = command_list[2]
        if sub_command not in self.allow_sub_commands:
            raise CommandNotAllowedException("%s: Unknown Command" % sub_command)

        return True

    def _raise_command_exception(self):
        command_tips = "Only Allow: "
        for command in self.allow_commands:
            command_tips += "%s," % " ".join(command)

        raise CommandNotAllowedException(command_tips.strip(","))
