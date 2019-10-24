#!/usr/bin/env python
# -*- coding=utf-8 -*-
#
# Author: huangnj
# Time: 2019/09/27

import traceback
from functools import wraps
from hdfs_kernel.constants import EXPECTED_ERROR_MSG, INTERNAL_ERROR_MSG
from hdfs.util import HdfsError

# == EXCEPTIONS ==
class SessionManagementException(Exception):
    pass

class CommandNotAllowedException(Exception):
    pass

class CommandExecuteException(Exception):
    pass


# option parse Error
class OptionParsingError(RuntimeError):
    pass

class OptionParsingExit(Exception):

    def __init__(self, status, msg):
        self.msg = msg
        self.status = status


# == DECORATORS FOR EXCEPTION HANDLING ==
EXPECTED_EXCEPTIONS = [HdfsError, SessionManagementException, CommandNotAllowedException,
                       CommandExecuteException, OptionParsingExit, OptionParsingError]


def handle_expected_exceptions(f):
    """A decorator that handles expected exceptions. Self can be any object with
    an "ipython_display" attribute.
    Usage:
    @handle_expected_exceptions
    def fn(self, ...):
        etc..."""
    exceptions_to_handle = tuple(EXPECTED_EXCEPTIONS)

    # Notice that we're NOT handling e.DataFrameParseException here. That's because DataFrameParseException
    # is an internal error that suggests something is wrong with LivyClientLib's implementation.
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        try:
            out = f(self, *args, **kwargs)
        except exceptions_to_handle as err:
            # Do not log! as some messages may contain private client information
            self.send_error(EXPECTED_ERROR_MSG.format(err))
            return None
        else:
            return out
    return wrapped


def wrap_unexpected_exceptions(f, execute_if_error=None):
    """A decorator that catches all exceptions from the function f and alerts the user about them.
    Self can be any object with a "logger" attribute and a "ipython_display" attribute.
    All exceptions are logged as "unexpected" exceptions, and a request is made to the user to file an issue
    at the Github repository. If there is an error, returns None if execute_if_error is None, or else
    returns the output of the function execute_if_error.
    Usage:
    @wrap_unexpected_exceptions
    def fn(self, ...):
        ..etc """
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        try:
            out = f(self, *args, **kwargs)
        except Exception as e:
            self.logger.error(u"ENCOUNTERED AN INTERNAL ERROR: {}\n\tTraceback:\n{}".format(e, traceback.format_exc()))
            self.send_error(INTERNAL_ERROR_MSG.format(e))
            return None if execute_if_error is None else execute_if_error()
        else:
            return out

    return wrapped
