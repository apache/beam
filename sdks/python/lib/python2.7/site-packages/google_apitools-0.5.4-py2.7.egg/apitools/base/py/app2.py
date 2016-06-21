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

"""Appcommands-compatible command class with extra fixins."""
from __future__ import print_function

import cmd
import inspect
import pdb
import shlex
import sys
import traceback
import types

import six

from google.apputils import app
from google.apputils import appcommands
import gflags as flags

__all__ = [
    'NewCmd',
    'Repl',
]

flags.DEFINE_boolean(
    'debug_mode', False,
    'Show tracebacks on Python exceptions.')
flags.DEFINE_boolean(
    'headless', False,
    'Assume no user is at the controlling console.')
FLAGS = flags.FLAGS


def _SafeMakeAscii(s):
    if isinstance(s, six.text_type):
        return s.encode('ascii')
    elif isinstance(s, str):
        return s.decode('ascii')
    else:
        return six.text_type(s).encode('ascii', 'backslashreplace')


class NewCmd(appcommands.Cmd):

    """Featureful extension of appcommands.Cmd."""

    def __init__(self, name, flag_values):
        super(NewCmd, self).__init__(name, flag_values)
        run_with_args = getattr(self, 'RunWithArgs', None)
        self._new_style = isinstance(run_with_args, types.MethodType)
        if self._new_style:
            func = run_with_args.__func__

            argspec = inspect.getargspec(func)
            if argspec.args and argspec.args[0] == 'self':
                argspec = argspec._replace(  # pylint: disable=protected-access
                    args=argspec.args[1:])
            self._argspec = argspec
            # TODO(craigcitro): Do we really want to support all this
            # nonsense?
            self._star_args = self._argspec.varargs is not None
            self._star_kwds = self._argspec.keywords is not None
            self._max_args = len(self._argspec.args or ())
            self._min_args = self._max_args - len(self._argspec.defaults or ())
            if self._star_args:
                self._max_args = sys.maxsize

            self._debug_mode = FLAGS.debug_mode
            self.surface_in_shell = True
            self.__doc__ = self.RunWithArgs.__doc__

    def __getattr__(self, name):
        if name in self._command_flags:
            return self._command_flags[name].value
        return super(NewCmd, self).__getattribute__(name)

    def _GetFlag(self, flagname):
        if flagname in self._command_flags:
            return self._command_flags[flagname]
        else:
            return None

    def Run(self, argv):
        """Run this command.

        If self is a new-style command, we set up arguments and call
        self.RunWithArgs, gracefully handling exceptions. If not, we
        simply call self.Run(argv).

        Args:
          argv: List of arguments as strings.

        Returns:
          0 on success, nonzero on failure.
        """
        if not self._new_style:
            return super(NewCmd, self).Run(argv)

        # TODO(craigcitro): We need to save and restore flags each time so
        # that we can per-command flags in the REPL.
        args = argv[1:]
        fail = None
        fail_template = '%s positional args, found %d, expected at %s %d'
        if len(args) < self._min_args:
            fail = fail_template % ('Not enough', len(args),
                                    'least', self._min_args)
        if len(args) > self._max_args:
            fail = fail_template % ('Too many', len(args),
                                    'most', self._max_args)
        if fail:
            print(fail)
            if self.usage:
                print('Usage: %s' % (self.usage,))
            return 1

        if self._debug_mode:
            return self.RunDebug(args, {})
        else:
            return self.RunSafely(args, {})

    def RunCmdLoop(self, argv):
        """Hook for use in cmd.Cmd-based command shells."""
        try:
            args = shlex.split(argv)
        except ValueError as e:
            raise SyntaxError(self.EncodeForPrinting(e))
        return self.Run([self._command_name] + args)

    @staticmethod
    def EncodeForPrinting(s):
        """Safely encode a string as the encoding for sys.stdout."""
        encoding = sys.stdout.encoding or 'ascii'
        return six.text_type(s).encode(encoding, 'backslashreplace')

    def _FormatError(self, e):
        """Hook for subclasses to modify how error messages are printed."""
        return _SafeMakeAscii(e)

    def _HandleError(self, e):
        message = self._FormatError(e)
        print('Exception raised in %s operation: %s' % (
            self._command_name, message))
        return 1

    def _IsDebuggableException(self, e):
        """Hook for subclasses to skip debugging on certain exceptions."""
        return not isinstance(e, app.UsageError)

    def RunDebug(self, args, kwds):
        """Run this command in debug mode."""
        try:
            return_value = self.RunWithArgs(*args, **kwds)
        except BaseException as e:
            # Don't break into the debugger for expected exceptions.
            if not self._IsDebuggableException(e):
                return self._HandleError(e)
            print()
            print('****************************************************')
            print('**   Unexpected Exception raised in execution!    **')
            if FLAGS.headless:
                print('**  --headless mode enabled, exiting.             **')
                print('**  See STDERR for traceback.                     **')
            else:
                print('**  --debug_mode enabled, starting pdb.           **')
            print('****************************************************')
            print()
            traceback.print_exc()
            print()
            if not FLAGS.headless:
                pdb.post_mortem()
            return 1
        return return_value

    def RunSafely(self, args, kwds):
        """Run this command, turning exceptions into print statements."""
        try:
            return_value = self.RunWithArgs(*args, **kwds)
        except BaseException as e:
            return self._HandleError(e)
        return return_value


class CommandLoop(cmd.Cmd):

    """Instance of cmd.Cmd built to work with NewCmd."""

    class TerminateSignal(Exception):

        """Exception type used for signaling loop completion."""

    def __init__(self, commands, prompt):
        cmd.Cmd.__init__(self)
        self._commands = {'help': commands['help']}
        self._special_command_names = ['help', 'repl', 'EOF']
        for name, command in commands.items():
            if (name not in self._special_command_names and
                    isinstance(command, NewCmd) and
                    command.surface_in_shell):
                self._commands[name] = command
                setattr(self, 'do_%s' % (name,), command.RunCmdLoop)
        self._default_prompt = prompt
        self._set_prompt()
        self._last_return_code = 0

    @property
    def last_return_code(self):
        return self._last_return_code

    def _set_prompt(self):
        self.prompt = self._default_prompt

    def do_EOF(self, *unused_args):  # pylint: disable=invalid-name
        """Terminate the running command loop.

        This function raises an exception to avoid the need to do
        potentially-error-prone string parsing inside onecmd.

        Args:
          *unused_args: unused.

        Returns:
          Never returns.

        Raises:
          CommandLoop.TerminateSignal: always.
        """
        raise CommandLoop.TerminateSignal()

    def postloop(self):
        print('Goodbye.')

    # pylint: disable=arguments-differ
    def completedefault(self, unused_text, line, unused_begidx, unused_endidx):
        if not line:
            return []
        else:
            command_name = line.partition(' ')[0].lower()
            usage = ''
            if command_name in self._commands:
                usage = self._commands[command_name].usage
            if usage:
                print()
                print(usage)
                print('%s%s' % (self.prompt, line), end=' ')
            return []
    # pylint: enable=arguments-differ

    def emptyline(self):
        print('Available commands:', end=' ')
        print(' '.join(list(self._commands)))

    def precmd(self, line):
        """Preprocess the shell input."""
        if line == 'EOF':
            return line
        if line.startswith('exit') or line.startswith('quit'):
            return 'EOF'
        words = line.strip().split()
        if len(words) == 1 and words[0] not in ['help', 'ls', 'version']:
            return 'help %s' % (line.strip(),)
        return line

    def onecmd(self, line):
        """Process a single command.

        Runs a single command, and stores the return code in
        self._last_return_code. Always returns False unless the command
        was EOF.

        Args:
          line: (str) Command line to process.

        Returns:
          A bool signaling whether or not the command loop should terminate.
        """
        try:
            self._last_return_code = cmd.Cmd.onecmd(self, line)
        except CommandLoop.TerminateSignal:
            return True
        except BaseException as e:
            name = line.split(' ')[0]
            print('Error running %s:' % name)
            print(e)
            self._last_return_code = 1
        return False

    def get_names(self):
        names = dir(self)
        commands = (name for name in self._commands
                    if name not in self._special_command_names)
        names.extend('do_%s' % (name,) for name in commands)
        names.remove('do_EOF')
        return names

    def do_help(self, command_name):
        """Print the help for command_name (if present) or general help."""

        # TODO(craigcitro): Add command-specific flags.
        def FormatOneCmd(name, command, command_names):
            indent_size = appcommands.GetMaxCommandLength() + 3
            if len(command_names) > 1:
                indent = ' ' * indent_size
                command_help = flags.TextWrap(
                    command.CommandGetHelp('', cmd_names=command_names),
                    indent=indent,
                    firstline_indent='')
                first_help_line, _, rest = command_help.partition('\n')
                first_line = '%-*s%s' % (indent_size,
                                         name + ':', first_help_line)
                return '\n'.join((first_line, rest))
            else:
                default_indent = '  '
                return '\n' + flags.TextWrap(
                    command.CommandGetHelp('', cmd_names=command_names),
                    indent=default_indent,
                    firstline_indent=default_indent) + '\n'

        if not command_name:
            print('\nHelp for commands:\n')
            command_names = list(self._commands)
            print('\n\n'.join(
                FormatOneCmd(name, command, command_names)
                for name, command in self._commands.items()
                if name not in self._special_command_names))
            print()
        elif command_name in self._commands:
            print(FormatOneCmd(command_name, self._commands[command_name],
                               command_names=[command_name]))
        return 0

    def postcmd(self, stop, line):
        return bool(stop) or line == 'EOF'


class Repl(NewCmd):

    """Start an interactive session."""
    PROMPT = '> '

    def __init__(self, name, fv):
        super(Repl, self).__init__(name, fv)
        self.surface_in_shell = False
        flags.DEFINE_string(
            'prompt', '',
            'Prompt to use for interactive shell.',
            flag_values=fv)

    def RunWithArgs(self):
        """Start an interactive session."""
        prompt = FLAGS.prompt or self.PROMPT
        repl = CommandLoop(appcommands.GetCommandList(), prompt=prompt)
        print('Welcome! (Type help for more information.)')
        while True:
            try:
                repl.cmdloop()
                break
            except KeyboardInterrupt:
                print()
        return repl.last_return_code
