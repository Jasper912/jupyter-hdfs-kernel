#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/25

from optparse import OptionParser
import re
from .paths import is_hdfs_path, HdfsPath, is_local_path
from hdfs_kernel.exceptions import OptionParsingExit, OptionParsingError


class CustomOptionParser(OptionParser):

    def error(self, msg):
        raise OptionParsingError(msg)

    def exit(self, status=0, msg=None):
        raise OptionParsingExit(status, msg)


class OptionParserBase(object):

    command = None

    def __init__(self):
        self.error = []

    def parse(self):
        raise NotImplemented

    def result_format(self, args, options):
        result = {
            "command": self.command,
            "args": args,
            "options": options.__dict__ if options else {},
            "error": self.error
        }
        return result

    def _parse_multi_path_args(self, args):
        paths = []
        error = "%s: '%%s': No such file or directory" % self.command
        for arg in args:
            if is_hdfs_path(arg):
                paths.append(HdfsPath(arg))

            elif is_local_path(arg):
                paths.append(arg)

            else:
                self.error.append(error % arg)

        return paths


class ListOptionParser(OptionParserBase):

    command = "-ls"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        opt_parser.add_option('-h', '--humanized', action='store_true', default=False, dest='humanized')

        # -C show path only
        opt_parser.add_option('-C', '--show_path_only', action='store_true', default=False, dest='show_path_only')
        # -R Recursively list the contents of directories
        #opt_parser.add_option('-R', '--recursively', action='store_true', default=False, dest='recursively')
        # -t  Sort files by modification time (most recent first).
        opt_parser.add_option('-t', '--sort_by_time', action='store_true', default=False, dest='sort_by_time')
        # -S  Sort files by size.
        opt_parser.add_option('-S', '--sort_by_size', action='store_true', default=False, dest='sort_by_size')
        # -r  Reverse the order of the sort.
        opt_parser.add_option('-r', '--reverse_sort', action='store_true', default=False, dest='reverse_sort')
        options, args = opt_parser.parse_args(options_list)
        hdfs_paths = self._parse_multi_path_args(args)

        _args = (hdfs_paths, )

        return self.result_format(_args, options)


class DuOptionParser(OptionParserBase):

    command = "-du"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        opt_parser.add_option('-h', '--humanized', action='store_true', default=False, dest='humanized')
        opt_parser.add_option('-s', '--summary', action='store_true', default=False, dest='summary')
        options, args = opt_parser.parse_args(options_list)

        hdfs_paths = self._parse_multi_path_args(args)
        _args = (hdfs_paths, )

        return self.result_format(_args, options)


class GetOptionParser(OptionParserBase):

    command = "-get"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        options, args = opt_parser.parse_args(options_list)

        paths = self._parse_multi_path_args(args)
        src_path = None
        dest_path = None
        if len(paths) == 1:
            # only src path
            src_path = paths.pop()
        elif len(paths) == 2:
            # src path and destination path
            src_path, dest_path = tuple(paths)
        else:
            self.error.append("command should be: -get <src> <localdst>")

        _args = (src_path, dest_path)

        return self.result_format(_args, options)


class PutOptionParser(OptionParserBase):

    command = "-put"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        options, args = opt_parser.parse_args(options_list)

        paths = self._parse_multi_path_args(args)
        src_path = None
        dest_path = None
        if len(paths) == 2:
            # src path and destination path
            src_path, dest_path = tuple(paths)
        else:
            self.error.append("command should be: -put <local path> <dest>")

        _args = (src_path, dest_path)

        return self.result_format(_args, options)


class MkdirOptionParser(OptionParserBase):

    command = "-mkdir"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        # -p  Do not fail if the directory already exists
        opt_parser.add_option('-p', '--permit', action='store_true', default=False, dest='permit')
        options, args = opt_parser.parse_args(options_list)
        paths = self._parse_multi_path_args(args)
        _args = (paths, )

        return self.result_format(_args, options)


class CopyOptionParser(OptionParserBase):

    command = "-cp"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        opt_parser.add_option('-f', '--force', action='store_true', default=False, dest='force')
        options, args = opt_parser.parse_args(options_list)
        paths = self._parse_multi_path_args(args)

        src_path = None
        dest_path = None
        if len(paths) == 2:
            # src path and destination path
            src_path, dest_path = tuple(paths)
        else:
            self.error.append("command should be: -cp [-f] <src> <dest>")

        _args = (src_path, dest_path)

        return self.result_format(_args, options)


class MoveOptionParser(OptionParserBase):

    command = "-mv"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        options, args = opt_parser.parse_args(options_list)
        paths = self._parse_multi_path_args(args)

        src_path = None
        dest_path = None
        if len(paths) == 2:
            src_path, dest_path = tuple(paths)
        else:
            self.error.append("command should be: -mv <src> <dest>")

        _args = (src_path, dest_path)

        return self.result_format(_args, options)


class RemoveOptionParser(OptionParserBase):

    command = "-rm"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        opt_parser.add_option('-f', '--force', action='store_true', default=False, dest='force')
        opt_parser.add_option('-r', '--recursively', action='store_true', default=False, dest='recursively')
        options, args = opt_parser.parse_args(options_list)
        paths = self._parse_multi_path_args(args)

        _args = (paths, )

        return self.result_format(_args, options)


class ChangeModeOptionParser(OptionParserBase):

    command = "-chmod"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        #opt_parser.add_option('-R', '--recursively', action='store_true', default=False, dest='recursively')
        options, args = opt_parser.parse_args(options_list)
        if len(args) != 2:
            raise OptionParsingError("-chmod: Not enough arguments: expected 2 but got %s" % len(args))

        octal_mode, path_args = tuple(args)
        pattern = r"[0-7]{3}"

        if len(octal_mode) != 3 or not re.findall(pattern, octal_mode):
            raise OptionParsingError("-chmod:  mode '%s' does not match the expected pattern." % octal_mode)

        paths = self._parse_multi_path_args([path_args])
        _args = (octal_mode, paths, )

        return self.result_format(_args, options)


class ChangeOwnerOptionParser(OptionParserBase):

    command = "-chown"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        #opt_parser.add_option('-R', '--recursively', action='store_true', default=False, dest='recursively')
        options, args = opt_parser.parse_args(options_list)
        if len(args) != 2:
            raise OptionParsingError("-chown: Not enough arguments: expected 2 but got %s" % len(args))

        owner, path_args = tuple(args)

        paths = self._parse_multi_path_args([path_args])
        _args = (owner, paths, )

        return self.result_format(_args, options)


class ChangeGroupOptionParser(OptionParserBase):

    command = "-chgrp"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        #opt_parser.add_option('-R', '--recursively', action='store_true', default=False, dest='recursively')
        options, args = opt_parser.parse_args(options_list)
        if len(args) != 2:
            raise OptionParsingError("-chgrp: Not enough arguments: expected 2 but got %s" % len(args))

        group, path_args = tuple(args)

        paths = self._parse_multi_path_args([path_args])
        _args = (group, paths, )

        return self.result_format(_args, options)


class CountOptionParser(OptionParserBase):

    command = "-count"

    def parse(self, options_list):
        opt_parser = CustomOptionParser(add_help_option=False)
        opt_parser.add_option('-q', '--quota', action='store_true', default=False, dest='quota')
        opt_parser.add_option('-h', '--humanized', action='store_true', default=False, dest='humanized')
        options, args = opt_parser.parse_args(options_list)

        paths = self._parse_multi_path_args(args)
        _args = (paths, )

        return self.result_format(_args, options)


class HelpOptionParser(OptionParserBase):

    command = "-help"
    def parse(self, options_list):
        return self.result_format((), {})


class OptionParserProxy(object):
    mapper = {
        "-ls": ListOptionParser,
        "-du": DuOptionParser,
        "-get": GetOptionParser,
        "-put": PutOptionParser,
        "-copyFromLocal": PutOptionParser,
        "-mkdir": MkdirOptionParser,
        "-cp": CopyOptionParser,
        "-mv": MoveOptionParser,
        "-rm": RemoveOptionParser,
        "-chmod": ChangeModeOptionParser,
        "-chown": ChangeOwnerOptionParser,
        "-chgrp": ChangeGroupOptionParser,
        "-count": CountOptionParser,
        "-help": HelpOptionParser
    }

    def __init__(self, sub_command, options_list):
        self.sub_command = sub_command
        assert sub_command in self.mapper.keys(), \
            "%s option parser not found" % sub_command
        self.parser = self.mapper.get(sub_command)
        self.options_list = options_list

    def parse(self):
        return self.parser().parse(self.options_list)
