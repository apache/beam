#!/usr/bin/env python
#
# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Base script for generated CLI."""

import atexit
import code
import logging
import os
import readline
import rlcompleter
import sys

from google.apputils import appcommands
import gflags as flags

from apitools.base.py import encoding
from apitools.base.py import exceptions

__all__ = [
    'ConsoleWithReadline',
    'DeclareBaseFlags',
    'FormatOutput',
    'SetupLogger',
    'run_main',
]


# TODO(craigcitro): We should move all the flags for the
# StandardQueryParameters into this file, so that they can be used
# elsewhere easily.

_BASE_FLAGS_DECLARED = False
_OUTPUT_FORMATTER_MAP = {
    'protorpc': lambda x: x,
    'json': encoding.MessageToJson,
}


def DeclareBaseFlags():
    """Declare base flags for all CLIs."""
    # TODO(craigcitro): FlagValidators?
    global _BASE_FLAGS_DECLARED  # pylint: disable=global-statement
    if _BASE_FLAGS_DECLARED:
        return
    flags.DEFINE_boolean(
        'log_request', False,
        'Log requests.')
    flags.DEFINE_boolean(
        'log_response', False,
        'Log responses.')
    flags.DEFINE_boolean(
        'log_request_response', False,
        'Log requests and responses.')
    flags.DEFINE_enum(
        'output_format',
        'protorpc',
        _OUTPUT_FORMATTER_MAP.keys(),
        'Display format for results.')

    _BASE_FLAGS_DECLARED = True

FLAGS = flags.FLAGS


def SetupLogger():
    if FLAGS.log_request or FLAGS.log_response or FLAGS.log_request_response:
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)


def FormatOutput(message, output_format=None):
    """Convert the output to the user-specified format."""
    output_format = output_format or FLAGS.output_format
    formatter = _OUTPUT_FORMATTER_MAP.get(FLAGS.output_format)
    if formatter is None:
        raise exceptions.UserError('Unknown output format: %s' % output_format)
    return formatter(message)


class _SmartCompleter(rlcompleter.Completer):

    def _callable_postfix(self, val, word):
        if ('(' in readline.get_line_buffer() or
                not callable(val)):
            return word
        else:
            return word + '('

    def complete(self, text, state):
        if not readline.get_line_buffer().strip():
            if not state:
                return '  '
            else:
                return None
        return rlcompleter.Completer.complete(self, text, state)


class ConsoleWithReadline(code.InteractiveConsole):

    """InteractiveConsole with readline, tab completion, and history."""

    def __init__(self, env, filename='<console>', histfile=None):
        new_locals = dict(env)
        new_locals.update({
            '_SmartCompleter': _SmartCompleter,
            'readline': readline,
            'rlcompleter': rlcompleter,
        })
        code.InteractiveConsole.__init__(self, new_locals, filename)
        readline.parse_and_bind('tab: complete')
        readline.set_completer(_SmartCompleter(new_locals).complete)
        if histfile is not None:
            histfile = os.path.expanduser(histfile)
            if os.path.exists(histfile):
                readline.read_history_file(histfile)
            atexit.register(lambda: readline.write_history_file(histfile))


def run_main():  # pylint: disable=invalid-name
    """Function to be used as setuptools script entry point.

    Appcommands assumes that it always runs as __main__, but launching
    via a setuptools-generated entry_point breaks this rule. We do some
    trickery here to make sure that appcommands and flags find their
    state where they expect to by faking ourselves as __main__.
    """

    # Put the flags for this module somewhere the flags module will look
    # for them.
    # pylint: disable=protected-access
    new_name = flags._GetMainModule()
    sys.modules[new_name] = sys.modules['__main__']
    for flag in FLAGS.FlagsByModuleDict().get(__name__, []):
        FLAGS._RegisterFlagByModule(new_name, flag)
        for key_flag in FLAGS.KeyFlagsByModuleDict().get(__name__, []):
            FLAGS._RegisterKeyFlagForModule(new_name, key_flag)
    # pylint: enable=protected-access

    # Now set __main__ appropriately so that appcommands will be
    # happy.
    sys.modules['__main__'] = sys.modules[__name__]
    appcommands.Run()
    sys.modules['__main__'] = sys.modules.pop(new_name)


if __name__ == '__main__':
    appcommands.Run()
