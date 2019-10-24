#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/25

import os
import re
import pandas
import getpass
from datetime import datetime
from pandas import DataFrame
from hdfs_kernel.parsers.paths import HdfsPath
from hdfs_kernel.utils import tools
import hdfs_kernel.utils.configuration as config
from hdfs_kernel.exceptions import CommandExecuteException
from collections import OrderedDict
from hdfs_kernel.constants import HDFS_FILE_TYPE, HDFS_DIRECTORY_TYPE

# set dataframe max_colwidth
pandas.set_option("max_colwidth", 240)



class CommandResult(dict):

    def __init__(self, data=None, status=True, message=None):
        item = {
            "status": status,
            "data": data,
            "message": message
        }
        super(CommandResult, self).__init__(item)


class CommandBase(object):

    def __init__(self, session_manager):
        self.session_manager = session_manager

    def execute(self):
        raise NotImplemented

    def pack_format(self, result):
        return self._trans_to_dataframe(result)

    def _trans_to_dataframe(self, records):
        df = DataFrame.from_records(records)
        return df

    def _timestamp_to_str(self, timestamp):
        return datetime.fromtimestamp(timestamp/1000) \
                .strftime("%Y-%m-%d %H:%M:%S")

    def _get_full_path(self, path, path_suffix):
        return os.path.join(path, path_suffix)

    def _build_local_path(self, path):
        user_workspace = config.user_workspace()
        # default notebook workspace
        if not path or path == ".":
            local_path = user_workspace
        elif path.startswith("./"):
            # path like ./aa.txt
            local_path = os.path.join(user_workspace, path.replace("./", "", 1))
        else:
            local_path = path

        local_path = os.path.join(user_workspace, path)
        return local_path

    def _make_dir(self, path, nameservice):
        return self.session_manager(nameservice).makedirs(path)

    def _get_path_detail(self, path, nameservice):
        client = self.session_manager(nameservice)
        status = client.status(path, strict=False)
        return status

    def _get_list_detail_by_hdfs_path(self, path, fields=None):
        """
            @params path hdfs_path
            @params fields FileStatus fields
            @return detail list
        """
        client = self.session_manager(path['nameservice'])
        result = client.list(path['path'], status=True)
        return self._get_result_detail(result, fields=fields)

    def _get_result_detail(self, result, fields=None, show_path_only=False,
                           recursively=False, sort_by_time=False,
                           sort_by_size=False, reverse_sort=False, **kwargs):
        """
            only return path detail info
            @params: result list of FileStatus
            @return: detail list
        """
        if sort_by_time:
            fields.append("modificationTime")

        if sort_by_size:
            fields.append("length")

        if recursively:
            pass

        data = []
        for item in result:
            _, detail = item
            if 'modificationTime' in detail:
                detail['modificationTime'] = self._timestamp_to_str(detail['modificationTime'])

            d = OrderedDict()
            if fields:
                for field in fields:
                    d[field] = detail[field]
                data.append(d)
            else:
                data.append(detail)

        if sort_by_time:
            data = sorted(data, key=lambda detail: detail['modificationTime'])

        if sort_by_size:
            data = sorted(data, key=lambda detail: detail['length'])

        if reverse_sort:
            data.reverse()

        if show_path_only:
            data = [{"pathSuffix": item['pathSuffix']} for item in data]

        return data


    def copy_hdfs_path(self, src, dest, overwrite=False):
        """
            copy hdfs file
            @params src HdfsPath
            @params dest HdfsPath
            @return True or Exception
        """

        if not isinstance(src, HdfsPath) or \
                not isinstance(dest, HdfsPath):
            raise CommandExecuteException("get wrong type of hdfs path")

        src_detail = self._get_path_detail(src['path'], src['nameservice'])
        if not src_detail:
            raise CommandExecuteException("-cp: `%s` No such file or directory" % src['path'])

        if src_detail['type'] == HDFS_DIRECTORY_TYPE:
            # scan dir
            detail_list = self._get_list_detail_by_hdfs_path(src, fields=['pathSuffix', 'type'])
            for item in detail_list:
                src_path_next = "{path_service}{path}/{path_suffix}".format(
                    path_service=src['path_service'],
                    path=src['path'],
                    path_suffix=item['pathSuffix']
                )
                dest_path_next = "{path_service}{path}/{path_suffix}".format(
                    path_service=dest['path_service'],
                    path=dest['path'],
                    path_suffix=item['pathSuffix']
                )
                if item['type'] == HDFS_DIRECTORY_TYPE:
                    self.copy_hdfs_path(HdfsPath(src_path_next), HdfsPath(dest_path_next), overwrite=overwrite)
                elif item['type'] == HDFS_FILE_TYPE:
                    self._copy_hdfs_file(HdfsPath(src_path_next), HdfsPath(dest_path_next), overwrite=overwrite)
                else:
                    continue
        elif src_detail['type'] == HDFS_FILE_TYPE:
            self._copy_hdfs_file(src, dest, overwrite=overwrite)

        return True

    def _copy_hdfs_file(self, src, dest, overwrite=False):
        chunk_size = 1024 * 1024 * 4

        src_detail = self._get_path_detail(src['path'], src['nameservice'])
        if src_detail['type'] != HDFS_FILE_TYPE:
            raise CommandExecuteException("-cp: Only support copy file")

        filename = os.path.basename(src['path'])

        dest_detail = self._get_path_detail(dest['path'], dest['nameservice'])
        if not dest_detail:
            # path not exists, maybe is a file path
            parent_path = os.path.dirname(dest['path'])
            _parent_detail = self._get_path_detail(parent_path, dest['nameservice'])
            if not _parent_detail:
                # parent dir not exists, mkdir first
                self._make_dir(parent_path, dest['nameservice'])

            save_path = dest['path']

        else:
            # path exists, check is a directory or file
            if dest_detail['type'] == HDFS_FILE_TYPE:
                save_path = dest['path']

            elif dest_detail['type'] == HDFS_DIRECTORY_TYPE:
                save_path = os.path.join(dest['path'], filename)
            else:
                raise CommandExecuteException("-cp: dest path `%s` is %s file type" \
                                              % (dest['source_path'], dest_detail['type']))


        src_client = self.session_manager(src['nameservice'])
        dest_client = self.session_manager(dest['nameservice'])
        with src_client.read(src['path'], chunk_size=chunk_size) as reader_generator:
            dest_client.write(
                save_path,
                data=reader_generator,
                overwrite=overwrite
            )

        return True

    def delete_hdfs_path(self, path, recursively=False, **kwargs):
        """
            delete hdfs path
            @params path HdfsPath
            @params recursively
            @params force
            @return boolean
        """
        hdfs_path = path.get("path")
        client = self.session_manager(path['nameservice'])
        is_success = client.delete(hdfs_path, recursive=recursively)
        return is_success

    def move_to_trash(self, path):
        hdfs_path = path.get("path")
        client = self.session_manager(path['nameservice'])

        filename = os.path.basename(hdfs_path)
        parent_path = os.path.dirname(hdfs_path)
        if parent_path.startswith('/'):
            parent_path = parent_path[1:]

        username = getpass.getuser()
        _trash_basepath = f'/user/{username}/.Trash/Current'
        _trash_path = os.path.join(_trash_basepath, parent_path)

        _trash_path_status = self._get_path_detail(_trash_path, path['nameservice'])
        if not _trash_path_status:
            # file not exists
            client.makedirs(_trash_path)

        client.rename(hdfs_path, os.path.join(_trash_path, filename))
        return True



class ListCommand(CommandBase):
    """
        hdfs dfs -ls path
        @return DataFrame
    """

    def execute(self, hdfs_paths, **kwargs):
        df = DataFrame()
        for path in hdfs_paths:
            if not isinstance(path, HdfsPath):
                continue

            real_path = path.get("path")
            source_path = path.get("source_path")

            client = self.session_manager(path['nameservice'])
            response = client.list(real_path, status=True)

            df_tmp = self.pack_format(response, source_path, **kwargs)
            df = df.append(df_tmp)

        return CommandResult(data=df)

    def pack_format(self, result, source_path, **kwargs):
        fields = ["type", "permission", "replication", "owner",
                  "group", "length", "modificationTime", "pathSuffix"]

        data = []
        humanized = kwargs.get("humanized", False)
        detail_list = self._get_result_detail(result, fields=fields, **kwargs)
        for detail in detail_list:
            path_suffix = detail.pop("pathSuffix")
            detail['path'] = self._get_full_path(source_path, path_suffix)

            if humanized and 'length' in detail:
                detail['length'] = tools.convert_size_readable(detail['length'])

            data.append(detail)

        return self._trans_to_dataframe(data)


class DuCommand(ListCommand):
    """
        disk space used
        hdfs dfs -du path
    """

    def pack_format(self, result, source_path, **kwargs):
        fields = ["length", "replication", "pathSuffix"]
        detail_list = self._get_result_detail(result, fields=fields, **kwargs)

        humanized = kwargs.get("humanized", False)
        summary = kwargs.get("summary", False)
        if summary:
            return self._summary_format(detail_list, source_path, humanized)

        data = []
        for detail in detail_list:
            path_suffix = detail.pop("pathSuffix")
            detail['path'] = self._get_full_path(source_path, path_suffix)

            if humanized:
                detail['replicationLength'] = tools.convert_size_readable(detail['length'] * detail['replication'])
                detail['length'] = tools.convert_size_readable(detail['length'])

            data.append(detail)

        return self._trans_to_dataframe(data)

    def _summary_format(self, detail_list, source_path, humanized):
        length = sum([i['length'] for i in detail_list])
        replication_length = sum([i['length'] * i['replication'] for i in detail_list])

        if humanized:
            length = tools.convert_size_readable(length)
            replication_length = tools.convert_size_readable(replication_length)

        data = {
            "length": length,
            "replicationLength": replication_length,
            "path": source_path
        }
        return self._trans_to_dataframe([data])


class GetCommand(CommandBase):
    """
        Download hdfs file
        hdfs dfs -get path
    """

    def execute(self, src, dest, **kwargs):
        # FIXME check file size restriction

        user_workspace = config.user_workspace()

        if isinstance(dest, HdfsPath):
            dest = dest.get("path")

        local_path = self._build_local_path(dest)

        hdfs_path = src.get("path")
        client = self.session_manager(src['nameservice'])
        save_path = client.download(hdfs_path, local_path, overwrite=True, chunk_size=1024 * 1024 * 4)
        result = "Save Path: %s" % save_path
        return CommandResult(data=result)


class PutCommand(CommandBase):
    """
        Upload File
        upload local path to hdfs
    """
    def execute(self, src, dest, **kwargs):
        if isinstance(src, HdfsPath):
            src = src.get("path")

        local_path = self._build_local_path(src)

        hdfs_path = dest.get("path")
        client = self.session_manager(dest['nameservice'])
        remote_path = client.upload(hdfs_path, local_path, overwrite=False)

        result = "Upload Path: %s" % remote_path
        return CommandResult(data=result)


class MkdirCommand(CommandBase):
    """
        make dir hdfs path
    """
    def execute(self, paths, **kwargs):
        permit = kwargs.get("permit")
        message = ""
        for path in paths:
            hdfs_path = path.get("path")
            source_path = path.get("source_path")
            client = self.session_manager(path['nameservice'])

            status_info = client.status(hdfs_path, strict=False)
            if status_info:
                # path already exists, skip
                message += "mkdir: `%s` File exists </br>" % source_path
                continue

            client.makedirs(hdfs_path)

        message = message or "Success"
        return CommandResult(data=message)


class CopyCommand(CommandBase):
    """
        copy file/directory from <src> to <dest>
    """

    def execute(self, src, dest, **kwargs):
        overwrite = kwargs.get("force")
        self.copy_hdfs_path(src, dest, overwrite=overwrite)
        return CommandResult(data="Success")


class MoveCommand(CommandBase):
    """
        move hdfs path
    """
    def execute(self, src, dest, **kwargs):
        if not isinstance(src, HdfsPath) or \
                not isinstance(dest, HdfsPath):
            raise CommandExecuteException("get wrong type of hdfs path")

        if src['nameservice'] != dest['nameservice']:
            raise CommandExecuteException("could not move path in different name service")

        client = self.session_manager(src['nameservice'])
        client.rename(src['path'], dest['path'])
        return CommandResult(data="Success")


class RemoveCommand(CommandBase):
    """
        remove -r -f hdfs path
    """
    def execute(self, paths, **kwargs):
        for path in paths:
            status = self._get_path_detail(path['path'], path['nameservice'])
            if not status:
                raise CommandExecuteException("-rm: `%s` No such file or directory" % path['path'])

            # move to trash
            self.move_to_trash(path)

            #self.delete_hdfs_path(path, **kwargs)

        return CommandResult(data="Success")

class ChmodCommand(CommandBase):
    """
        -chmod octal_mode path
    """
    def execute(self, octal_mode, paths, **kwargs):
        for path in paths:
            client = self.session_manager(path['nameservice'])
            client.set_permission(path['path'], octal_mode)

        return CommandResult(data="Success")


class ChownCommand(CommandBase):
    """
        -chown user[:group] path
    """
    def execute(self, owner, paths, **kwargs):
        pattern = r"(\w+):(\w+)"
        for path in paths:
            match = re.findall(pattern, owner)

            group = None
            if match and len(re.findall(":", owner)) == 1:
                owner, group = match.pop()

            client = self.session_manager(path['nameservice'])
            client.set_owner(path['path'], owner=owner, group=group)
        return CommandResult(data="Success")


class ChgrpCommand(CommandBase):
    """
        -chgrp group path
    """
    def execute(self, group, paths, **kwargs):
        for path in paths:
            client = self.session_manager(path['nameservice'])
            client.set_owner(path['path'], group=group)
        return CommandResult(data="Success")


class CountCommand(CommandBase):
    """
        -count get path Content
    """
    def execute(self, paths, **kwargs):
        quota = kwargs.get("quota", False)
        humanized = kwargs.get("humanized", False)

        fields = []
        if quota:
            fields.extend(["quota", "spaceQuota", "spaceConsumed"])
        fields.extend(["directoryCount", "fileCount", "length"])

        data = []
        for path in paths:
            client = self.session_manager(path['nameservice'])
            response = client.content(path['path'])

            d = OrderedDict()
            for field in fields:
                d[field] = response[field]

            if humanized:
                d['length'] = tools.convert_size_readable(response['length'])
                if "spaceConsumed" in fields and response['spaceConsumed']:
                    d['spaceConsumed'] = tools.convert_size_readable(response['spaceConsumed'])

            d['path'] = path['source_path']

            data.append(d)

        df = self._trans_to_dataframe(data)
        return CommandResult(data=df)


class CommandDispatcher(object):

    _map = {
        "-ls": ListCommand,
        "-du": DuCommand,
        "-get": GetCommand,
        "-put": PutCommand,
        "-copyFromLocal": PutCommand,
        "-mkdir": MkdirCommand,
        "-cp": CopyCommand,
        "-mv": MoveCommand,
        "-rm": RemoveCommand,
        "-chmod": ChmodCommand,
        "-chown": ChownCommand,
        "-chgrp": ChgrpCommand,
        "-count": CountCommand
    }

    def __init__(self, command, session_manager):
        assert command in self._map, "Command Not Found"
        self.executer = self._map.get(command)(session_manager)

    def execute(self, *args, **kwargs):
        return self.executer.execute(*args, **kwargs)
