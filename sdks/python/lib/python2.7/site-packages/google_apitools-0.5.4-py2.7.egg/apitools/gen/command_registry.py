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

"""Command registry for apitools."""

import logging
import textwrap

from apitools.base.protorpclite import descriptor
from apitools.base.protorpclite import messages
from apitools.gen import extended_descriptor

# This is a code generator; we're purposely verbose.
# pylint:disable=too-many-statements

_VARIANT_TO_FLAG_TYPE_MAP = {
    messages.Variant.DOUBLE: 'float',
    messages.Variant.FLOAT: 'float',
    messages.Variant.INT64: 'string',
    messages.Variant.UINT64: 'string',
    messages.Variant.INT32: 'integer',
    messages.Variant.BOOL: 'boolean',
    messages.Variant.STRING: 'string',
    messages.Variant.MESSAGE: 'string',
    messages.Variant.BYTES: 'string',
    messages.Variant.UINT32: 'integer',
    messages.Variant.ENUM: 'enum',
    messages.Variant.SINT32: 'integer',
    messages.Variant.SINT64: 'integer',
}


class FlagInfo(messages.Message):

    """Information about a flag and conversion to a message.

    Fields:
      name: name of this flag.
      type: type of the flag.
      description: description of the flag.
      default: default value for this flag.
      enum_values: if this flag is an enum, the list of possible
          values.
      required: whether or not this flag is required.
      fv: name of the flag_values object where this flag should
          be registered.
      conversion: template for type conversion.
      special: (boolean, default: False) If True, this flag doesn't
          correspond to an attribute on the request.
    """
    name = messages.StringField(1)
    type = messages.StringField(2)
    description = messages.StringField(3)
    default = messages.StringField(4)
    enum_values = messages.StringField(5, repeated=True)
    required = messages.BooleanField(6, default=False)
    fv = messages.StringField(7)
    conversion = messages.StringField(8)
    special = messages.BooleanField(9, default=False)


class ArgInfo(messages.Message):

    """Information about a single positional command argument.

    Fields:
      name: argument name.
      description: description of this argument.
      conversion: template for type conversion.
    """
    name = messages.StringField(1)
    description = messages.StringField(2)
    conversion = messages.StringField(3)


class CommandInfo(messages.Message):

    """Information about a single command.

    Fields:
      name: name of this command.
      class_name: name of the apitools_base.NewCmd class for this command.
      description: description of this command.
      flags: list of FlagInfo messages for the command-specific flags.
      args: list of ArgInfo messages for the positional args.
      request_type: name of the request type for this command.
      client_method_path: path from the client object to the method
          this command is wrapping.
    """
    name = messages.StringField(1)
    class_name = messages.StringField(2)
    description = messages.StringField(3)
    flags = messages.MessageField(FlagInfo, 4, repeated=True)
    args = messages.MessageField(ArgInfo, 5, repeated=True)
    request_type = messages.StringField(6)
    client_method_path = messages.StringField(7)
    has_upload = messages.BooleanField(8, default=False)
    has_download = messages.BooleanField(9, default=False)


class CommandRegistry(object):

    """Registry for CLI commands."""

    def __init__(self, package, version, client_info, message_registry,
                 root_package, base_files_package, protorpc_package, names):
        self.__package = package
        self.__version = version
        self.__client_info = client_info
        self.__names = names
        self.__message_registry = message_registry
        self.__root_package = root_package
        self.__base_files_package = base_files_package
        self.__protorpc_package = protorpc_package
        self.__command_list = []
        self.__global_flags = []

    def Validate(self):
        self.__message_registry.Validate()

    def AddGlobalParameters(self, schema):
        for field in schema.fields:
            self.__global_flags.append(self.__FlagInfoFromField(field, schema))

    def AddCommandForMethod(self, service_name, method_name, method_info,
                            request, _):
        """Add the given method as a command."""
        command_name = self.__GetCommandName(method_info.method_id)
        calling_path = '%s.%s' % (service_name, method_name)
        request_type = self.__message_registry.LookupDescriptor(request)
        description = method_info.description
        if not description:
            description = 'Call the %s method.' % method_info.method_id
        field_map = dict((f.name, f) for f in request_type.fields)
        args = []
        arg_names = []
        for field_name in method_info.ordered_params:
            extended_field = field_map[field_name]
            name = extended_field.name
            args.append(ArgInfo(
                name=name,
                description=extended_field.description,
                conversion=self.__GetConversion(extended_field, request_type),
            ))
            arg_names.append(name)
        flags = []
        for extended_field in sorted(request_type.fields,
                                     key=lambda x: x.name):
            field = extended_field.field_descriptor
            if extended_field.name in arg_names:
                continue
            if self.__FieldIsRequired(field):
                logging.warning(
                    'Required field %s not in ordered_params for command %s',
                    extended_field.name, command_name)
            flags.append(self.__FlagInfoFromField(
                extended_field, request_type, fv='fv'))
        if method_info.upload_config:
            # TODO(craigcitro): Consider adding additional flags to allow
            # determining the filename from the object metadata.
            upload_flag_info = FlagInfo(
                name='upload_filename', type='string', default='',
                description='Filename to use for upload.', fv='fv',
                special=True)
            flags.append(upload_flag_info)
            mime_description = (
                'MIME type to use for the upload. Only needed if '
                'the extension on --upload_filename does not determine '
                'the correct (or any) MIME type.')
            mime_type_flag_info = FlagInfo(
                name='upload_mime_type', type='string', default='',
                description=mime_description, fv='fv', special=True)
            flags.append(mime_type_flag_info)
        if method_info.supports_download:
            download_flag_info = FlagInfo(
                name='download_filename', type='string', default='',
                description='Filename to use for download.', fv='fv',
                special=True)
            flags.append(download_flag_info)
            overwrite_description = (
                'If True, overwrite the existing file when downloading.')
            overwrite_flag_info = FlagInfo(
                name='overwrite', type='boolean', default='False',
                description=overwrite_description, fv='fv', special=True)
            flags.append(overwrite_flag_info)
        command_info = CommandInfo(
            name=command_name,
            class_name=self.__names.ClassName(command_name),
            description=description,
            flags=flags,
            args=args,
            request_type=request_type.full_name,
            client_method_path=calling_path,
            has_upload=bool(method_info.upload_config),
            has_download=bool(method_info.supports_download)
        )
        self.__command_list.append(command_info)

    def __LookupMessage(self, message, field):
        message_type = self.__message_registry.LookupDescriptor(
            '%s.%s' % (message.name, field.type_name))
        if message_type is None:
            message_type = self.__message_registry.LookupDescriptor(
                field.type_name)
        return message_type

    def __GetCommandName(self, method_id):
        command_name = method_id
        prefix = '%s.' % self.__package
        if command_name.startswith(prefix):
            command_name = command_name[len(prefix):]
        command_name = command_name.replace('.', '_')
        return command_name

    def __GetConversion(self, extended_field, extended_message):
        field = extended_field.field_descriptor

        type_name = ''
        if field.variant in (messages.Variant.MESSAGE, messages.Variant.ENUM):
            if field.type_name.startswith('apitools.base.protorpclite.'):
                type_name = field.type_name
            else:
                field_message = self.__LookupMessage(extended_message, field)
                if field_message is None:
                    raise ValueError(
                        'Could not find type for field %s' % field.name)
                type_name = 'messages.%s' % field_message.full_name

        template = ''
        if field.variant in (messages.Variant.INT64, messages.Variant.UINT64):
            template = 'int(%s)'
        elif field.variant == messages.Variant.MESSAGE:
            template = 'apitools_base.JsonToMessage(%s, %%s)' % type_name
        elif field.variant == messages.Variant.ENUM:
            template = '%s(%%s)' % type_name
        elif field.variant == messages.Variant.STRING:
            template = "%s.decode('utf8')"

        if self.__FieldIsRepeated(extended_field.field_descriptor):
            if template:
                template = '[%s for x in %%s]' % (template % 'x')

        return template

    def __FieldIsRequired(self, field):
        return field.label == descriptor.FieldDescriptor.Label.REQUIRED

    def __FieldIsRepeated(self, field):
        return field.label == descriptor.FieldDescriptor.Label.REPEATED

    def __FlagInfoFromField(self, extended_field, extended_message, fv=''):
        field = extended_field.field_descriptor
        flag_info = FlagInfo()
        flag_info.name = str(field.name)
        # TODO(craigcitro): We should key by variant.
        flag_info.type = _VARIANT_TO_FLAG_TYPE_MAP[field.variant]
        flag_info.description = extended_field.description
        if field.default_value:
            # TODO(craigcitro): Formatting?
            flag_info.default = field.default_value
        if flag_info.type == 'enum':
            # TODO(craigcitro): Does protorpc do this for us?
            enum_type = self.__LookupMessage(extended_message, field)
            if enum_type is None:
                raise ValueError('Cannot find enum type %s', field.type_name)
            flag_info.enum_values = [x.name for x in enum_type.values]
            # Note that this choice is completely arbitrary -- but we only
            # push the value through if the user specifies it, so this
            # doesn't hurt anything.
            if flag_info.default is None:
                flag_info.default = flag_info.enum_values[0]
        if self.__FieldIsRequired(field):
            flag_info.required = True
        flag_info.fv = fv
        flag_info.conversion = self.__GetConversion(
            extended_field, extended_message)
        return flag_info

    def __PrintFlagDeclarations(self, printer):
        package = self.__client_info.package
        function_name = '_Declare%sFlags' % (package[0].upper() + package[1:])
        printer()
        printer()
        printer('def %s():', function_name)
        with printer.Indent():
            printer('"""Declare global flags in an idempotent way."""')
            printer("if 'api_endpoint' in flags.FLAGS:")
            with printer.Indent():
                printer('return')
            printer('flags.DEFINE_string(')
            with printer.Indent('    '):
                printer("'api_endpoint',")
                printer('%r,', self.__client_info.base_url)
                printer("'URL of the API endpoint to use.',")
                printer("short_name='%s_url')", self.__package)
            printer('flags.DEFINE_string(')
            with printer.Indent('    '):
                printer("'history_file',")
                printer('%r,', '~/.%s.%s.history' %
                        (self.__package, self.__version))
                printer("'File with interactive shell history.')")
            printer('flags.DEFINE_multistring(')
            with printer.Indent('    '):
                printer("'add_header', [],")
                printer("'Additional http headers (as key=value strings). '")
                printer("'Can be specified multiple times.')")
            printer('flags.DEFINE_string(')
            with printer.Indent('    '):
                printer("'service_account_json_keyfile', '',")
                printer("'Filename for a JSON service account key downloaded'")
                printer("' from the Developer Console.')")
            for flag_info in self.__global_flags:
                self.__PrintFlag(printer, flag_info)
        printer()
        printer()
        printer('FLAGS = flags.FLAGS')
        printer('apitools_base_cli.DeclareBaseFlags()')
        printer('%s()', function_name)

    def __PrintGetGlobalParams(self, printer):
        printer('def GetGlobalParamsFromFlags():')
        with printer.Indent():
            printer('"""Return a StandardQueryParameters based on flags."""')
            printer('result = messages.StandardQueryParameters()')

            for flag_info in self.__global_flags:
                rhs = 'FLAGS.%s' % flag_info.name
                if flag_info.conversion:
                    rhs = flag_info.conversion % rhs
                printer('if FLAGS[%r].present:', flag_info.name)
                with printer.Indent():
                    printer('result.%s = %s', flag_info.name, rhs)
            printer('return result')
        printer()
        printer()

    def __PrintGetClient(self, printer):
        printer('def GetClientFromFlags():')
        with printer.Indent():
            printer('"""Return a client object, configured from flags."""')
            printer('log_request = FLAGS.log_request or '
                    'FLAGS.log_request_response')
            printer('log_response = FLAGS.log_response or '
                    'FLAGS.log_request_response')
            printer('api_endpoint = apitools_base.NormalizeApiEndpoint('
                    'FLAGS.api_endpoint)')
            printer("additional_http_headers = dict(x.split('=', 1) for x in "
                    "FLAGS.add_header)")
            printer('credentials_args = {')
            with printer.Indent('    '):
                printer("'service_account_json_keyfile': os.path.expanduser("
                        'FLAGS.service_account_json_keyfile)')
            printer('}')
            printer('try:')
            with printer.Indent():
                printer('client = client_lib.%s(',
                        self.__client_info.client_class_name)
                with printer.Indent(indent='    '):
                    printer('api_endpoint, log_request=log_request,')
                    printer('log_response=log_response,')
                    printer('credentials_args=credentials_args,')
                    printer('additional_http_headers=additional_http_headers)')
            printer('except apitools_base.CredentialsError as e:')
            with printer.Indent():
                printer("print 'Error creating credentials: %%s' %% e")
                printer('sys.exit(1)')
            printer('return client')
        printer()
        printer()

    def __PrintCommandDocstring(self, printer, command_info):
        with printer.CommentContext():
            for line in textwrap.wrap('"""%s' % command_info.description,
                                      printer.CalculateWidth()):
                printer(line)
            extended_descriptor.PrintIndentedDescriptions(
                printer, command_info.args, 'Args')
            extended_descriptor.PrintIndentedDescriptions(
                printer, command_info.flags, 'Flags')
            printer('"""')

    def __PrintFlag(self, printer, flag_info):
        printer('flags.DEFINE_%s(', flag_info.type)
        with printer.Indent(indent='    '):
            printer('%r,', flag_info.name)
            printer('%r,', flag_info.default)
            if flag_info.type == 'enum':
                printer('%r,', flag_info.enum_values)

            # TODO(craigcitro): Consider using 'drop_whitespace' elsewhere.
            description_lines = textwrap.wrap(
                flag_info.description, 75 - len(printer.indent),
                drop_whitespace=False)
            for line in description_lines[:-1]:
                printer('%r', line)
            last_line = description_lines[-1] if description_lines else ''
            printer('%r%s', last_line, ',' if flag_info.fv else ')')
            if flag_info.fv:
                printer('flag_values=%s)', flag_info.fv)
        if flag_info.required:
            printer('flags.MarkFlagAsRequired(%r)', flag_info.name)

    def __PrintPyShell(self, printer):
        printer('class PyShell(appcommands.Cmd):')
        printer()
        with printer.Indent():
            printer('def Run(self, _):')
            with printer.Indent():
                printer(
                    '"""Run an interactive python shell with the client."""')
                printer('client = GetClientFromFlags()')
                printer('params = GetGlobalParamsFromFlags()')
                printer('for field in params.all_fields():')
                with printer.Indent():
                    printer('value = params.get_assigned_value(field.name)')
                    printer('if value != field.default:')
                    with printer.Indent():
                        printer('client.AddGlobalParam(field.name, value)')
                printer('banner = """')
                printer('       == %s interactive console ==' % (
                    self.__client_info.package))
                printer('             client: a %s client' %
                        self.__client_info.package)
                printer('      apitools_base: base apitools module')
                printer('     messages: the generated messages module')
                printer('"""')
                printer('local_vars = {')
                with printer.Indent(indent='    '):
                    printer("'apitools_base': apitools_base,")
                    printer("'client': client,")
                    printer("'client_lib': client_lib,")
                    printer("'messages': messages,")
                printer('}')
                printer("if platform.system() == 'Linux':")
                with printer.Indent():
                    printer('console = apitools_base_cli.ConsoleWithReadline(')
                    with printer.Indent(indent='    '):
                        printer('local_vars, histfile=FLAGS.history_file)')
                printer('else:')
                with printer.Indent():
                    printer('console = code.InteractiveConsole(local_vars)')
                printer('try:')
                with printer.Indent():
                    printer('console.interact(banner)')
                printer('except SystemExit as e:')
                with printer.Indent():
                    printer('return e.code')
        printer()
        printer()

    def WriteFile(self, printer):
        """Write a simple CLI (currently just a stub)."""
        printer('#!/usr/bin/env python')
        printer('"""CLI for %s, version %s."""',
                self.__package, self.__version)
        printer('# NOTE: This file is autogenerated and should not be edited '
                'by hand.')
        # TODO(craigcitro): Add a build stamp, along with some other
        # information.
        printer()
        printer('import code')
        printer('import os')
        printer('import platform')
        printer('import sys')
        printer()
        printer('from %s import message_types', self.__protorpc_package)
        printer('from %s import messages', self.__protorpc_package)
        printer()
        appcommands_import = 'from google.apputils import appcommands'
        printer(appcommands_import)

        flags_import = 'import gflags as flags'
        printer(flags_import)
        printer()
        printer('import %s as apitools_base', self.__base_files_package)
        printer('from %s import cli as apitools_base_cli',
                self.__base_files_package)
        import_prefix = ''
        printer('%simport %s as client_lib',
                import_prefix, self.__client_info.client_rule_name)
        printer('%simport %s as messages',
                import_prefix, self.__client_info.messages_rule_name)
        self.__PrintFlagDeclarations(printer)
        printer()
        printer()
        self.__PrintGetGlobalParams(printer)
        self.__PrintGetClient(printer)
        self.__PrintPyShell(printer)
        self.__PrintCommands(printer)
        printer('def main(_):')
        with printer.Indent():
            printer("appcommands.AddCmd('pyshell', PyShell)")
            for command_info in self.__command_list:
                printer("appcommands.AddCmd('%s', %s)",
                        command_info.name, command_info.class_name)
            printer()
            printer('apitools_base_cli.SetupLogger()')
            # TODO(craigcitro): Just call SetDefaultCommand as soon as
            # another appcommands release happens and this exists
            # externally.
            printer("if hasattr(appcommands, 'SetDefaultCommand'):")
            with printer.Indent():
                printer("appcommands.SetDefaultCommand('pyshell')")
        printer()
        printer()
        printer('run_main = apitools_base_cli.run_main')
        printer()
        printer("if __name__ == '__main__':")
        with printer.Indent():
            printer('appcommands.Run()')

    def __PrintCommands(self, printer):
        """Print all commands in this registry using printer."""
        for command_info in self.__command_list:
            arg_list = [arg_info.name for arg_info in command_info.args]
            printer(
                'class %s(apitools_base_cli.NewCmd):', command_info.class_name)
            with printer.Indent():
                printer('"""Command wrapping %s."""',
                        command_info.client_method_path)
                printer()
                printer('usage = """%s%s%s"""',
                        command_info.name,
                        ' ' if arg_list else '',
                        ' '.join('<%s>' % argname for argname in arg_list))
                printer()
                printer('def __init__(self, name, fv):')
                with printer.Indent():
                    printer('super(%s, self).__init__(name, fv)',
                            command_info.class_name)
                    for flag in command_info.flags:
                        self.__PrintFlag(printer, flag)
                printer()
                printer('def RunWithArgs(%s):', ', '.join(['self'] + arg_list))
                with printer.Indent():
                    self.__PrintCommandDocstring(printer, command_info)
                    printer('client = GetClientFromFlags()')
                    printer('global_params = GetGlobalParamsFromFlags()')
                    printer(
                        'request = messages.%s(', command_info.request_type)
                    with printer.Indent(indent='    '):
                        for arg in command_info.args:
                            rhs = arg.name
                            if arg.conversion:
                                rhs = arg.conversion % arg.name
                            printer('%s=%s,', arg.name, rhs)
                        printer(')')
                    for flag_info in command_info.flags:
                        if flag_info.special:
                            continue
                        rhs = 'FLAGS.%s' % flag_info.name
                        if flag_info.conversion:
                            rhs = flag_info.conversion % rhs
                        printer('if FLAGS[%r].present:', flag_info.name)
                        with printer.Indent():
                            printer('request.%s = %s', flag_info.name, rhs)
                    call_args = ['request', 'global_params=global_params']
                    if command_info.has_upload:
                        call_args.append('upload=upload')
                        printer('upload = None')
                        printer('if FLAGS.upload_filename:')
                        with printer.Indent():
                            printer('upload = apitools_base.Upload.FromFile(')
                            printer('    FLAGS.upload_filename, '
                                    'FLAGS.upload_mime_type,')
                            printer('    progress_callback='
                                    'apitools_base.UploadProgressPrinter,')
                            printer('    finish_callback='
                                    'apitools_base.UploadCompletePrinter)')
                    if command_info.has_download:
                        call_args.append('download=download')
                        printer('download = None')
                        printer('if FLAGS.download_filename:')
                        with printer.Indent():
                            printer('download = apitools_base.Download.'
                                    'FromFile(FLAGS.download_filename, '
                                    'overwrite=FLAGS.overwrite,')
                            printer('    progress_callback='
                                    'apitools_base.DownloadProgressPrinter,')
                            printer('    finish_callback='
                                    'apitools_base.DownloadCompletePrinter)')
                    printer(
                        'result = client.%s(', command_info.client_method_path)
                    with printer.Indent(indent='    '):
                        printer('%s)', ', '.join(call_args))
                    printer('print apitools_base_cli.FormatOutput(result)')
            printer()
            printer()
