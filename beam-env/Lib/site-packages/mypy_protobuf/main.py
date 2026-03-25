#!/usr/bin/env python
"""Protoc Plugin to generate mypy stubs."""
from __future__ import annotations

import ast
import enum
import sys
from collections import defaultdict
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

import google.protobuf.descriptor_pb2 as d
from google.protobuf.compiler import plugin_pb2 as plugin_pb2
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from google.protobuf.internal.well_known_types import WKTBASES

from . import extensions_pb2

__version__ = "5.0.0"

# SourceCodeLocation is defined by `message Location` here
# https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/descriptor.proto
SourceCodeLocation = List[int]

# So phabricator doesn't think mypy_protobuf.py is generated
GENERATED = "@ge" + "nerated"
HEADER = f"""
{GENERATED} by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""

# See https://github.com/nipunn1313/mypy-protobuf/issues/73 for details
PYTHON_RESERVED = {
    "False",
    "None",
    "True",
    "and",
    "as",
    "async",
    "await",
    "assert",
    "break",
    "class",
    "continue",
    "def",
    "del",
    "elif",
    "else",
    "except",
    "finally",
    "for",
    "from",
    "global",
    "if",
    "import",
    "in",
    "is",
    "lambda",
    "nonlocal",
    "not",
    "or",
    "pass",
    "raise",
    "return",
    "try",
    "while",
    "with",
    "yield",
}

PROTO_ENUM_RESERVED = {
    "Name",
    "Value",
    "keys",
    "values",
    "items",
}


def _mangle_global_identifier(name: str) -> str:
    """
    Module level identifiers are mangled and aliased so that they can be disambiguated
    from fields/enum variants with the same name within the file.

    Eg:
    Enum variant `Name` or message field `Name` might conflict with a top level
    message or enum named `Name`, so mangle it with a `Global___` prefix for
    internal references. Note that this doesn't affect inner enums/messages
    because they get fully qualified when referenced within a file"""
    return f"Global___{name}"


class Descriptors(object):
    def __init__(self, request: plugin_pb2.CodeGeneratorRequest) -> None:
        files = {f.name: f for f in request.proto_file}
        to_generate = {n: files[n] for n in request.file_to_generate}
        self.files: Dict[str, d.FileDescriptorProto] = files
        self.to_generate: Dict[str, d.FileDescriptorProto] = to_generate
        self.messages: Dict[str, d.DescriptorProto] = {}
        self.message_to_fd: Dict[str, d.FileDescriptorProto] = {}

        def _add_enums(
            enums: "RepeatedCompositeFieldContainer[d.EnumDescriptorProto]",
            prefix: str,
            _fd: d.FileDescriptorProto,
        ) -> None:
            for enum_proto in enums:
                self.message_to_fd[prefix + enum_proto.name] = _fd
                self.message_to_fd[prefix + enum_proto.name + ".ValueType"] = _fd

        def _add_messages(
            messages: "RepeatedCompositeFieldContainer[d.DescriptorProto]",
            prefix: str,
            _fd: d.FileDescriptorProto,
        ) -> None:
            for message in messages:
                self.messages[prefix + message.name] = message
                self.message_to_fd[prefix + message.name] = _fd
                sub_prefix = prefix + message.name + "."
                _add_messages(message.nested_type, sub_prefix, _fd)
                _add_enums(message.enum_type, sub_prefix, _fd)

        for fd in request.proto_file:
            start_prefix = "." + fd.package + "." if fd.package else "."
            _add_messages(fd.message_type, start_prefix, fd)
            _add_enums(fd.enum_type, start_prefix, fd)


class GRPCType(enum.Enum):
    SYNC = "SYNC"
    ASYNC = "ASYNC"
    BOTH = "BOTH"

    @classmethod
    def from_parameter(cls, parameter: str) -> GRPCType:
        has_sync = "only_sync" in parameter
        has_async = "only_async" in parameter

        if has_sync and has_async:
            raise ValueError("Cannot specify both only_sync and only_async")
        elif has_sync:
            return GRPCType.SYNC
        elif has_async:
            return GRPCType.ASYNC
        else:
            return GRPCType.BOTH

    @property
    def supports_sync(self) -> bool:
        return self in (GRPCType.SYNC, GRPCType.BOTH)

    @property
    def supports_async(self) -> bool:
        return self in (GRPCType.ASYNC, GRPCType.BOTH)


class Imports:
    # Set of {x: (a, b, c) that maps to either `from {a} import {b} as {c}` or `import {b} as {c}`
    _imports: Dict[str, Tuple[Optional[str], Optional[str], str]]
    # Set of imported names, if the same name is imported more than once it gets formatted as _{name}_{count}
    _imported_names: Dict[str, int] = defaultdict(int)
    # Set of imported typing statements, form of {<name>: <alias>}
    _typing_imports: Dict[str, str]

    def __init__(self) -> None:
        self._imports = {}
        self._imported_names = defaultdict(int)
        self._typing_imports = {}

    @property
    def statements(self) -> List[str]:
        """Returns a list of import tuples.

            0: From statement, optional
            1: Import name
            2: Alias

        Example:
            self._import("grpc.aio", "Channel") -> ("grpc", "aio", "_aio") -> from grpc import aio as _aio
            self._import("grpc", "Channel") -> (None, "grpc", "_grpc") -> import grpc as _grpc

        """
        statements = []
        for _, (frm, alias, original) in self._imports.items():
            if frm is None:
                if alias is None:
                    statements.append(f"import {original}")
                else:
                    statements.append(f"import {original} as {alias}")
            else:
                statements.append(f"from {frm} import {original} as {alias}")

        return statements

    @property
    def typing_statements(self) -> List[str]:
        """Return a dictionary of typing imports with their aliases."""
        statements = []
        for orig, alias in self._typing_imports.items():
            statements.append(f"{orig} as {alias}")
        return statements

    def _name_alias(self, name: str, mangle: bool = True) -> str:
        if f"{name}" in self._imported_names:
            count = self._imported_names[name] + 1
            self._imported_names[name] = count
            return f"{'_' if mangle else ''}{name}_{count}"
        else:
            self._imported_names[name] = 0
            return f"{'_' if mangle else ''}{name}"

    def add_import(self, path: str, name: Optional[str] = None, mangle: bool = True) -> str:
        """Add an import and return the name used for it

        Examples:
            add_import("collections.abc.Iterable", "Iterable") -> _abc.Iterable

        """
        if not name:
            self._imports[path] = (None, None, path)
            return path
        if imp := self._imports.get(path):
            _alias = imp[1]
        else:
            parts = path.rsplit(".", 1)
            if len(parts) > 1:
                _alias = self._name_alias(parts[-1], mangle=mangle)
                self._imports[path] = (parts[0], _alias, parts[-1])
            elif len(parts) == 1:
                _alias = self._name_alias(parts[0], mangle=mangle)
                self._imports[path] = (None, _alias, parts[0])
            else:
                raise RuntimeError("Package import string cannot be empty")

        return f"{_alias}.{name}"

    def add_typing_import(self, name: str) -> str:
        if name not in self._typing_imports:
            self._typing_imports[name] = self._name_alias(name)
        return self._typing_imports[name]


class PkgWriter(object):
    """Writes a single pyi file"""

    def __init__(
        self,
        fd: d.FileDescriptorProto,
        descriptors: Descriptors,
        readable_stubs: bool,
        relax_strict_optional_primitives: bool,
        use_default_deprecation_warnings: bool,
        generate_concrete_servicer_stubs: bool,
        grpc: bool,
        grpc_type: GRPCType = GRPCType.BOTH,
    ) -> None:
        self.fd = fd
        self.descriptors = descriptors
        self.readable_stubs = readable_stubs
        self.relax_strict_optional_primitives = relax_strict_optional_primitives
        self.use_default_depreaction_warnings = use_default_deprecation_warnings
        self.generate_concrete_servicer_stubs = generate_concrete_servicer_stubs
        self.grpc = grpc
        self.grpc_type = grpc_type
        self.lines: List[str] = []
        self.indent = ""

        # Set of {x: y}, where {x} corresponds to to `from {x} import {y} as _{y}`
        self.imports = Imports()
        self.typing_extensions_min: Optional[Tuple[int, int]] = None
        self.deprecated_min: Optional[Tuple[int, int]] = None

        # Comments
        self.source_code_info_by_scl = {tuple(location.path): location for location in fd.source_code_info.location}

    @property
    def _deprecated_name(self) -> str:
        return "_deprecated"

    def _import(self, path: str, name: str) -> str:
        """Imports a stdlib path and returns a handle to it
        eg. self._import("typing", "Literal") -> "Literal"

        If alias is true, then it will prefix the import with an underscore to prevent conflicts with builtin names
        """
        if path == "typing_extensions":
            stabilization = {"TypeAlias": (3, 10), "TypeVar": (3, 13), "type_check_only": (3, 12), "Self": (3, 11)}
            assert name in stabilization
            if not self.typing_extensions_min or self.typing_extensions_min < stabilization[name]:
                self.typing_extensions_min = stabilization[name]
            return self.imports.add_typing_import(name)

        if path == "warnings" and name == "deprecated":
            if not self.deprecated_min or self.deprecated_min < (3, 11):
                self.deprecated_min = (3, 13)
            return self._deprecated_name

        imp = path.replace("/", ".")
        if self.readable_stubs:
            # Do not mangle if readable stubs is set. This can cause conflicts
            return self.imports.add_import(imp, name, mangle=False)
        else:
            return self.imports.add_import(imp, name)

    def _property(self) -> str:
        return f"@{self._import('builtins', 'property')}"

    def _import_message(self, name: str) -> str:
        """Import a referenced message and return a handle"""
        message_fd = self.descriptors.message_to_fd[name]
        assert message_fd.name.endswith(".proto")

        # Strip off package name
        if message_fd.package:
            assert name.startswith("." + message_fd.package + ".")
            name = name[len("." + message_fd.package + ".") :]
        else:
            assert name.startswith(".")
            name = name[1:]

        # Use prepended "_r_" to disambiguate message names that alias python reserved keywords
        split = name.split(".")
        for i, part in enumerate(split):
            if part in PYTHON_RESERVED:
                split[i] = "_r_" + part
        name = ".".join(split)

        # Message defined in this file. Note: GRPC stubs in same .proto are generated into separate files
        if not self.grpc and message_fd.name == self.fd.name:
            return name if self.readable_stubs else _mangle_global_identifier(name)

        # Not in file. Must import
        # Python generated code ignores proto packages, so the only relevant factor is
        # whether it is in the file or not.
        import_name = self._import(message_fd.name[:-6].replace("-", "_") + "_pb2", split[0])

        remains = ".".join(split[1:])
        if not remains:
            return import_name

        # remains could either be a direct import of a nested enum or message
        # from another package.
        return import_name + "." + remains

    def _builtin(self, name: str) -> str:
        return self._import("builtins", name)

    @contextmanager
    def _indent(self) -> Iterator[None]:
        self.indent = self.indent + "    "
        yield
        self.indent = self.indent[:-4]

    def _write_line(self, line: str, *args: Any) -> None:
        if args:
            line = line.format(*args)
        if line == "":
            self.lines.append(line)
        else:
            self.lines.append(self.indent + line)

    def _break_text(self, text_block: str) -> List[str]:
        if text_block == "":
            return []
        return [line[1:] if line.startswith(" ") else line for line in text_block.rstrip().split("\n")]

    def _has_comments(self, scl: SourceCodeLocation) -> bool:
        sci_loc = self.source_code_info_by_scl.get(tuple(scl))
        return sci_loc is not None and bool(sci_loc.leading_detached_comments or sci_loc.leading_comments or sci_loc.trailing_comments)

    def _get_comments(self, scl: SourceCodeLocation) -> List[str]:
        """Return list of comment lines"""
        if not self._has_comments(scl):
            return []

        sci_loc = self.source_code_info_by_scl.get(tuple(scl))
        assert sci_loc is not None

        leading_detached_lines = []
        leading_lines = []
        trailing_lines = []
        for leading_detached_comment in sci_loc.leading_detached_comments:
            leading_detached_lines = self._break_text(leading_detached_comment)
        if sci_loc.leading_comments is not None:
            leading_lines = self._break_text(sci_loc.leading_comments)
        # Trailing comments also go in the header - to make sure it gets into the docstring
        if sci_loc.trailing_comments is not None:
            trailing_lines = self._break_text(sci_loc.trailing_comments)

        lines = leading_detached_lines
        if leading_detached_lines and (leading_lines or trailing_lines):
            lines.append("")
        lines.extend(leading_lines)
        lines.extend(trailing_lines)

        return lines

    def _get_deprecation_message(self, scl: SourceCodeLocation, default_message: str) -> str:
        msg = default_message
        if not self.use_default_depreaction_warnings and (comments := self._get_comments(scl)):
            # Make sure the comment string is a valid python string literal
            joined = "\\n".join(comments)
            # Check that it is valid python string by using ast.parse
            try:
                ast.parse(f'"""{joined}"""')
                msg = joined
            except SyntaxError as e:
                print(f"Warning: Deprecation comment {joined} could not be parsed as a python string literal. Using default deprecation message. {e}", file=sys.stderr)
                pass
        return msg

    def _write_deprecation_warning(self, scl: SourceCodeLocation, default_message: str) -> None:
        msg = self._get_deprecation_message(scl, default_message)
        self._write_line(
            '@{}("""{}""")',
            self._import("warnings", "deprecated"),
            msg,
        )

    def _write_comments(self, scl: SourceCodeLocation) -> bool:
        """Return true if any comments were written"""
        if not self._has_comments(scl):
            return False

        sci_loc = self.source_code_info_by_scl.get(tuple(scl))
        assert sci_loc is not None

        leading_detached_lines = []
        leading_lines = []
        trailing_lines = []
        for leading_detached_comment in sci_loc.leading_detached_comments:
            leading_detached_lines = self._break_text(leading_detached_comment)
        if sci_loc.leading_comments is not None:
            leading_lines = self._break_text(sci_loc.leading_comments)
        # Trailing comments also go in the header - to make sure it gets into the docstring
        if sci_loc.trailing_comments is not None:
            trailing_lines = self._break_text(sci_loc.trailing_comments)

        lines = leading_detached_lines
        if leading_detached_lines and (leading_lines or trailing_lines):
            lines.append("")
        lines.extend(leading_lines)
        lines.extend(trailing_lines)

        lines = [
            # Escape triple-quotes that would otherwise end the docstring early.
            line.replace("\\", "\\\\").replace('"""', r"\"\"\"")
            for line in lines
        ]
        if len(lines) == 1:
            line = lines[0]
            if line.endswith(('"', "\\")):
                # Docstrings are terminated with triple-quotes, so if the documentation itself ends in a quote,
                # insert some whitespace to separate it from the closing quotes.
                # This is not necessary with multiline comments
                # because in that case we always insert a newline before the trailing triple-quotes.
                line = line + " "
            self._write_line(f'"""{line}"""')
        else:
            for i, line in enumerate(lines):
                if i == 0:
                    self._write_line(f'"""{line}')
                else:
                    self._write_line(f"{line}")
            self._write_line('"""')

        return True

    def write_enum_values(
        self,
        values: Iterable[Tuple[int, d.EnumValueDescriptorProto]],
        value_type: str,
        scl_prefix: SourceCodeLocation,
        *,
        class_attributes: bool = False,
    ) -> None:
        for i, val in values:
            if val.name in PYTHON_RESERVED:
                continue

            scl = scl_prefix + [i]
            # Class level
            if class_attributes and val.options.deprecated:
                self._write_line(self._property())
                self._write_deprecation_warning(
                    scl + [d.EnumValueDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.EnumOptions.DEPRECATED_FIELD_NUMBER],
                    "This enum value has been marked as deprecated using proto enum value options.",
                )
                self._write_line(
                    f"def {val.name}(self) -> {value_type}: {'' if self._has_comments(scl) else '...'}  # {val.number}",
                )
                with self._indent():
                    self._write_comments(scl)
            # Module level or non-deprecated class level
            else:
                self._write_line(
                    f"{val.name}: {value_type}  # {val.number}",
                )
                self._write_comments(scl)

    def write_module_attributes(self) -> None:
        wl = self._write_line
        fd_type = self._import("google.protobuf.descriptor", "FileDescriptor")
        wl(f"DESCRIPTOR: {fd_type}")
        wl("")

    def write_enums(
        self,
        enums: Iterable[d.EnumDescriptorProto],
        prefix: str,
        scl_prefix: SourceCodeLocation,
    ) -> None:
        wl = self._write_line
        for i, enum_proto in enumerate(enums):
            class_name = enum_proto.name if enum_proto.name not in PYTHON_RESERVED else "_r_" + enum_proto.name
            value_type_fq = prefix + class_name + ".ValueType"
            enum_helper_class = "_" + enum_proto.name
            value_type_helper_fq = prefix + enum_helper_class + ".ValueType"
            etw_helper_class = "_" + enum_proto.name + "EnumTypeWrapper"
            scl = scl_prefix + [i]

            wl(f"class {enum_helper_class}:")
            with self._indent():
                wl(
                    'ValueType = {}("ValueType", {})',
                    self._import("typing", "NewType"),
                    self._builtin("int"),
                )
                # Alias to the classic shorter definition "V"
                wl("V: {} = ValueType  # noqa: Y015", self._import("typing_extensions", "TypeAlias"))
            wl("")
            wl(
                "class {}({}[{}], {}):",
                etw_helper_class,
                self._import("google.protobuf.internal.enum_type_wrapper", "_EnumTypeWrapper"),
                value_type_helper_fq,
                self._builtin("type"),
            )
            with self._indent():
                ed = self._import("google.protobuf.descriptor", "EnumDescriptor")
                wl(f"DESCRIPTOR: {ed}")
                self.write_enum_values(
                    [(i, v) for i, v in enumerate(enum_proto.value) if v.name not in PROTO_ENUM_RESERVED],
                    value_type_helper_fq,
                    scl + [d.EnumDescriptorProto.VALUE_FIELD_NUMBER],
                    class_attributes=True,
                )
            wl("")

            if enum_proto.options.deprecated:
                self._write_deprecation_warning(
                    scl + [d.EnumDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.EnumOptions.DEPRECATED_FIELD_NUMBER],
                    "This enum has been marked as deprecated using proto enum options.",
                )
            if self._has_comments(scl):
                wl(f"class {class_name}({enum_helper_class}, metaclass={etw_helper_class}):")
                with self._indent():
                    self._write_comments(scl)
                wl("")
            else:
                wl(f"class {class_name}({enum_helper_class}, metaclass={etw_helper_class}): ...")
                if prefix == "":
                    wl("")

            # Write the module level constants for enum values
            self.write_enum_values(
                enumerate(enum_proto.value),
                value_type_fq,
                scl + [d.EnumDescriptorProto.VALUE_FIELD_NUMBER],
            )
            if prefix == "" and not self.readable_stubs:
                wl(f"{_mangle_global_identifier(class_name)}: {self._import('typing_extensions', 'TypeAlias')} = {class_name}  # noqa: Y015")
            wl("")

    def write_messages(
        self,
        messages: Iterable[d.DescriptorProto],
        prefix: str,
        scl_prefix: SourceCodeLocation,
        file_field_presence: d.FeatureSet.FieldPresence.ValueType = d.FeatureSet.FieldPresence.FIELD_PRESENCE_UNKNOWN,
    ) -> None:
        wl = self._write_line

        for i, desc in enumerate(messages):
            qualified_name = prefix + desc.name

            # Reproduce some hardcoded logic from the protobuf implementation - where
            # some specific "well_known_types" generated protos to have additional
            # base classes
            addl_base = ""
            if self.fd.package + "." + desc.name in WKTBASES:
                # chop off the .proto - and import the well known type
                # eg `from google.protobuf.duration import Duration`
                well_known_type = WKTBASES[self.fd.package + "." + desc.name]
                addl_base = ", " + self._import(
                    "google.protobuf.internal.well_known_types",
                    well_known_type.__name__,
                )

            class_name = desc.name if desc.name not in PYTHON_RESERVED else "_r_" + desc.name
            message_class = self._import("google.protobuf.message", "Message")
            if desc.options.deprecated:
                self._write_deprecation_warning(
                    scl_prefix + [i] + [d.DescriptorProto.OPTIONS_FIELD_NUMBER] + [d.MessageOptions.DEPRECATED_FIELD_NUMBER],
                    "This message has been marked as deprecated using proto message options.",
                )
            wl("@{}", self._import("typing", "final"))
            wl(f"class {class_name}({message_class}{addl_base}):")
            with self._indent():
                scl = scl_prefix + [i]
                if self._write_comments(scl):
                    wl("")

                desc_type = self._import("google.protobuf.descriptor", "Descriptor")
                wl(f"DESCRIPTOR: {desc_type}")
                wl("")

                # Nested enums/messages
                self.write_enums(
                    desc.enum_type,
                    qualified_name + ".",
                    scl + [d.DescriptorProto.ENUM_TYPE_FIELD_NUMBER],
                )
                self.write_messages(desc.nested_type, qualified_name + ".", scl + [d.DescriptorProto.NESTED_TYPE_FIELD_NUMBER], file_field_presence=file_field_presence)

                # integer constants  for field numbers
                for f in desc.field:
                    wl(f"{f.name.upper()}_FIELD_NUMBER: {self._builtin('int')}")

                for idx, field in enumerate(desc.field):
                    if field.name in PYTHON_RESERVED:
                        continue
                    field_type = self.python_type(field)
                    if is_scalar(field) and field.label != d.FieldDescriptorProto.LABEL_REPEATED:
                        # Scalar non repeated fields are r/w, generate getter and setter if deprecated
                        scl_field = scl + [d.DescriptorProto.FIELD_FIELD_NUMBER, idx]
                        deprecation_scl_field = scl_field + [d.FieldDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.FieldOptions.DEPRECATED_FIELD_NUMBER]
                        if field.options.deprecated:
                            wl(self._property())
                            self._write_deprecation_warning(
                                deprecation_scl_field,
                                "This field has been marked as deprecated using proto field options.",
                            )
                            wl(f"def {field.name}(self) -> {field_type}:{' ...' if not self._has_comments(scl_field) else ''}")
                            if self._has_comments(scl_field):
                                with self._indent():
                                    self._write_comments(scl_field)
                                wl("")
                            wl(f"@{field.name}.setter")
                            self._write_deprecation_warning(
                                deprecation_scl_field,
                                "This field has been marked as deprecated using proto field options.",
                            )
                            wl(f"def {field.name}(self, value: {field_type}) -> None:{' ...' if not self._has_comments(scl_field) else ''}")
                            if self._has_comments(scl_field):
                                with self._indent():
                                    self._write_comments(scl_field)
                                wl("")
                        else:
                            wl(f"{field.name}: {field_type}")
                            self._write_comments(scl_field)

                for idx, field in enumerate(desc.field):
                    if field.name in PYTHON_RESERVED:
                        continue
                    field_type = self.python_type(field)
                    if not (is_scalar(field) and field.label != d.FieldDescriptorProto.LABEL_REPEATED):
                        # r/o Getters for non-scalar fields and scalar-repeated fields
                        scl_field = scl + [d.DescriptorProto.FIELD_FIELD_NUMBER, idx]
                        wl(self._property())
                        if field.options.deprecated:
                            self._write_deprecation_warning(
                                scl_field + [d.FieldDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.FieldOptions.DEPRECATED_FIELD_NUMBER],
                                "This field has been marked as deprecated using proto field options.",
                            )
                        wl(f"def {field.name}(self) -> {field_type}:{' ...' if not self._has_comments(scl_field) else ''}")
                        if self._has_comments(scl_field):
                            with self._indent():
                                self._write_comments(scl_field)
                            wl("")

                self.write_extensions(desc.extension, scl + [d.DescriptorProto.EXTENSION_FIELD_NUMBER])

                # Constructor
                wl("def __init__(")
                with self._indent():
                    if any(f.name == "self" for f in desc.field):
                        wl("self_,  # pyright: ignore[reportSelfClsParameterName]")
                    else:
                        wl("self,")
                with self._indent():
                    constructor_fields = [f for f in desc.field if f.name not in PYTHON_RESERVED]
                    if len(constructor_fields) > 0:
                        # Only positional args allowed
                        # See https://github.com/nipunn1313/mypy-protobuf/issues/71
                        wl("*,")
                    for field in constructor_fields:
                        implicit_presence = self.get_field_presence(file_field_presence, field.options.features) == d.FeatureSet.FieldPresence.IMPLICIT
                        field_type = self.python_type(field, generic_container=True)
                        if (implicit_presence and self.fd.syntax == "editions") or (self.fd.syntax == "proto3" and is_scalar(field) and field.label != d.FieldDescriptorProto.LABEL_REPEATED and not self.relax_strict_optional_primitives and not field.proto3_optional):
                            wl(f"{field.name}: {field_type} = ...,")
                        else:
                            wl(f"{field.name}: {field_type} | None = ...,")
                wl(") -> None: ...")

                self.write_stringly_typed_fields(desc, file_field_presence)

            if prefix == "" and not self.readable_stubs:
                wl("")
                wl(f"{_mangle_global_identifier(class_name)}: {self._import('typing_extensions', 'TypeAlias')} = {class_name}  # noqa: Y015")
            wl("")

    @staticmethod
    def get_field_presence(file_field_presence: d.FeatureSet.FieldPresence.ValueType, field_feature_set: d.FeatureSet) -> d.FeatureSet.FieldPresence.ValueType:
        presence = file_field_presence
        if field_feature_set.HasField("field_presence"):
            presence = field_feature_set.field_presence
        return presence

    def write_stringly_typed_fields(self, desc: d.DescriptorProto, file_field_presence: d.FeatureSet.FieldPresence.ValueType) -> None:
        """Type the stringly-typed methods as a Union[Literal, Literal ...]"""
        wl = self._write_line
        # HasField, ClearField, WhichOneof accepts both bytes/str
        # HasField only supports singular. ClearField supports repeated as well
        # In proto3, HasField only supports message fields and optional fields
        # HasField always supports oneof fields
        hf_fields = [f.name for f in desc.field if f.HasField("oneof_index") or (self.fd.syntax == "editions" and self.get_field_presence(file_field_presence, f.options.features) != d.FeatureSet.FieldPresence.IMPLICIT) or (f.label != d.FieldDescriptorProto.LABEL_REPEATED and (self.fd.syntax in ("proto2", "") or f.type == d.FieldDescriptorProto.TYPE_MESSAGE or f.proto3_optional))]
        cf_fields = [f.name for f in desc.field]
        wo_fields = {oneof.name: [f.name for f in desc.field if f.HasField("oneof_index") and f.oneof_index == idx] for idx, oneof in enumerate(desc.oneof_decl)}

        hf_fields.extend(wo_fields.keys())
        cf_fields.extend(wo_fields.keys())

        hf_fields_text = ", ".join(sorted(f'"{name}", b"{name}"' for name in hf_fields))
        cf_fields_text = ", ".join(sorted(f'"{name}", b"{name}"' for name in cf_fields))

        if not hf_fields and not cf_fields and not wo_fields:
            return

        if hf_fields:
            wl("_HasFieldArgType: {} = {}[{}]  # noqa: Y015", self._import("typing_extensions", "TypeAlias"), self._import("typing", "Literal"), hf_fields_text)
            wl(
                "def HasField(self, field_name: _HasFieldArgType) -> {}: ...",
                self._builtin("bool"),
            )
        if cf_fields:
            wl("_ClearFieldArgType: {} = {}[{}]  # noqa: Y015", self._import("typing_extensions", "TypeAlias"), self._import("typing", "Literal"), cf_fields_text)
            wl(
                "def ClearField(self, field_name: _ClearFieldArgType) -> None: ...",
            )

        # Write type aliases first so overloads are not interrupted
        for wo_field, members in sorted(wo_fields.items()):
            wl(
                "_WhichOneofReturnType_{}: {} = {}[{}]  # noqa: Y015",
                wo_field,
                self._import("typing_extensions", "TypeAlias"),
                self._import("typing", "Literal"),
                # Returns `str`
                ", ".join(f'"{m}"' for m in members),
            )
            wl(
                "_WhichOneofArgType_{}: {} = {}[{}]  # noqa: Y015",
                wo_field,
                self._import("typing_extensions", "TypeAlias"),
                self._import("typing", "Literal"),
                # Accepts both str and bytes
                f'"{wo_field}", b"{wo_field}"',
            )
        for wo_field, _ in sorted(wo_fields.items()):
            if len(wo_fields) > 1:
                wl("@{}", self._import("typing", "overload"))
            wl(
                "def WhichOneof(self, oneof_group: {}) -> {} | None: ...",
                f"_WhichOneofArgType_{wo_field}",
                f"_WhichOneofReturnType_{wo_field}",
            )

    def write_extensions(
        self,
        extensions: Sequence[d.FieldDescriptorProto],
        scl_prefix: SourceCodeLocation,
    ) -> None:
        wl = self._write_line

        for ext in extensions:
            wl(f"{ext.name.upper()}_FIELD_NUMBER: {self._builtin('int')}")

        for i, ext in enumerate(extensions):
            scl = scl_prefix + [i]

            wl(
                "{}: {}[{}, {}]",
                ext.name,
                self._import(
                    "google.protobuf.internal.extension_dict",
                    "_ExtensionFieldDescriptor",
                ),
                self._import_message(ext.extendee),
                self.python_type(ext),
            )
            self._write_comments(scl)

    def write_methods(
        self,
        service: d.ServiceDescriptorProto,
        class_name: str,
        is_abstract: bool,
        scl_prefix: SourceCodeLocation,
    ) -> None:
        wl = self._write_line
        wl(
            "DESCRIPTOR: {}",
            self._import("google.protobuf.descriptor", "ServiceDescriptor"),
        )
        methods = [(i, m) for i, m in enumerate(service.method) if m.name not in PYTHON_RESERVED]
        if not methods:
            wl("...")
        for i, method in methods:
            if is_abstract:
                wl("@{}", self._import("abc", "abstractmethod"))
            wl(f"def {method.name}(")
            with self._indent():
                wl(f"inst: {class_name},  # pyright: ignore[reportSelfClsParameterName]")
                wl(
                    "rpc_controller: {},",
                    self._import("google.protobuf.service", "RpcController"),
                )
                wl("request: {},", self._import_message(method.input_type))
                wl(
                    "callback: {}[[{}], None] | None{},",
                    self._import("collections.abc", "Callable"),
                    self._import_message(method.output_type),
                    "" if is_abstract else " = ...",
                )

            scl_method = scl_prefix + [d.ServiceDescriptorProto.METHOD_FIELD_NUMBER, i]
            wl(
                ") -> {}[{}]:{}",
                self._import("concurrent.futures", "Future"),
                self._import_message(method.output_type),
                " ..." if not self._has_comments(scl_method) else "",
            )
            if self._has_comments(scl_method):
                with self._indent():
                    if not self._write_comments(scl_method):
                        wl("...")
            wl("")

    def write_services(
        self,
        services: Iterable[d.ServiceDescriptorProto],
        scl_prefix: SourceCodeLocation,
    ) -> None:
        wl = self._write_line
        for i, service in enumerate(services):
            scl = scl_prefix + [i]
            class_name = service.name if service.name not in PYTHON_RESERVED else "_r_" + service.name
            # The service definition interface
            wl(
                "class {}({}, metaclass={}):",
                class_name,
                self._import("google.protobuf.service", "Service"),
                self._import("abc", "ABCMeta"),
            )
            # The servicer interface
            with self._indent():
                if self._write_comments(scl):
                    wl("")
                self.write_methods(service, class_name, is_abstract=True, scl_prefix=scl)

            # The stub client
            stub_class_name = service.name + "_Stub"
            wl("class {}({}):", stub_class_name, class_name)
            with self._indent():
                if self._write_comments(scl):
                    wl("")
                wl(
                    "def __init__(self, rpc_channel: {}) -> None: ...",
                    self._import("google.protobuf.service", "RpcChannel"),
                )
                self.write_methods(service, stub_class_name, is_abstract=False, scl_prefix=scl)

    def _import_casttype(self, casttype: str) -> str:
        split = casttype.split(".")
        assert len(split) == 2, "mypy_protobuf.[casttype,keytype,valuetype] is expected to be of format path/to/file.TypeInFile"
        pkg = split[0].replace("/", ".")
        return self._import(pkg, split[1])

    def _map_key_value_types(
        self,
        map_field: d.FieldDescriptorProto,
        key_field: d.FieldDescriptorProto,
        value_field: d.FieldDescriptorProto,
    ) -> Tuple[str, str]:
        oldstyle_keytype = map_field.options.Extensions[extensions_pb2.keytype]
        if oldstyle_keytype:
            print(f"Warning: Map Field {map_field.name}: (mypy_protobuf.keytype) is deprecated. Prefer (mypy_protobuf.options).keytype", file=sys.stderr)
        key_casttype = map_field.options.Extensions[extensions_pb2.options].keytype or oldstyle_keytype
        ktype = self._import_casttype(key_casttype) if key_casttype else self.python_type(key_field)

        oldstyle_valuetype = map_field.options.Extensions[extensions_pb2.valuetype]
        if oldstyle_valuetype:
            print(f"Warning: Map Field {map_field.name}: (mypy_protobuf.valuetype) is deprecated. Prefer (mypy_protobuf.options).valuetype", file=sys.stderr)
        value_casttype = map_field.options.Extensions[extensions_pb2.options].valuetype or map_field.options.Extensions[extensions_pb2.valuetype]
        vtype = self._import_casttype(value_casttype) if value_casttype else self.python_type(value_field)

        return ktype, vtype

    def _callable_type(self, method: d.MethodDescriptorProto, is_async: bool = False) -> str:
        module = "grpc.aio" if is_async else "grpc"
        if method.client_streaming:
            if method.server_streaming:
                return self._import(module, "StreamStreamMultiCallable")
            else:
                return self._import(module, "StreamUnaryMultiCallable")
        else:
            if method.server_streaming:
                return self._import(module, "UnaryStreamMultiCallable")
            else:
                return self._import(module, "UnaryUnaryMultiCallable")

    def _input_type(self, method: d.MethodDescriptorProto) -> str:
        result = self._import_message(method.input_type)
        return result

    def _servicer_input_type(self, method: d.MethodDescriptorProto) -> str:
        result = self._import_message(method.input_type)
        if method.client_streaming:
            # See write_grpc_iterator_type().
            if self.grpc_type == GRPCType.SYNC:
                result = f'{self._import("collections.abc", "Iterator")}[{result}]'
            elif self.grpc_type == GRPCType.ASYNC:
                result = f'{self._import("collections.abc", "AsyncIterator")}[{result}]'
            else:
                result = f"_MaybeAsyncIterator[{result}]"
        return result

    def _output_type(self, method: d.MethodDescriptorProto) -> str:
        result = self._import_message(method.output_type)
        return result

    def _servicer_output_type(self, method: d.MethodDescriptorProto) -> str:
        msg = self._import_message(method.output_type)

        # Don't actually call _import until the end. Otherwise we get unnecessary imports
        if method.server_streaming:
            # Union[Iterator[Resp], AsyncIterator[Resp]] is subtyped by Iterator[Resp] and AsyncIterator[Resp].
            # So both can be used in the covariant function return position.
            def sync() -> str:
                return f"{self._import('collections.abc', 'Iterator')}[{msg}]"

            def a_sync() -> str:
                return f"{self._import('collections.abc', 'AsyncIterator')}[{msg}]"

            def result() -> str:
                return f"{self._import('typing', 'Union')}[{sync()}, {a_sync()}]"

        else:
            # Union[Resp, Awaitable[Resp]] is subtyped by Resp and Awaitable[Resp].
            # So both can be used in the covariant function return position.
            # Awaitable[Resp] is equivalent to async def.
            def sync() -> str:
                return msg

            def a_sync() -> str:
                return f"{self._import('collections.abc', 'Awaitable')}[{msg}]"

            def result() -> str:
                return f"{self._import('typing', 'Union')}[{sync()}, {a_sync()}]"

        if self.grpc_type == GRPCType.SYNC:
            return sync()
        elif self.grpc_type == GRPCType.ASYNC:
            return a_sync()
        else:
            return result()

    def write_grpc_iterator_type(self) -> None:
        wl = self._write_line

        if self.grpc_type == GRPCType.SYNC:
            self._import("collections.abc", "Iterator")
        elif self.grpc_type == GRPCType.ASYNC:
            self._import("collections.abc", "AsyncIterator")
        else:
            # _MaybeAsyncIterator[Req] is supertyped by Iterator[Req] and AsyncIterator[Req].
            # So both can be used in the contravariant function parameter position.
            wl('_T = {}("_T")', self._import("typing", "TypeVar"))
            wl("")
            wl(
                "class _MaybeAsyncIterator({}[_T], {}[_T], metaclass={}): ...",
                self._import("collections.abc", "AsyncIterator"),
                self._import("collections.abc", "Iterator"),
                self._import("abc", "ABCMeta"),
            )
        wl("")

    def get_servicer_context_type(self, input_: str, output: str) -> str:
        """Get the type to use for the context parameter in servicer methods."""
        if self.grpc_type == GRPCType.ASYNC:
            return f"{self._import('grpc.aio', 'ServicerContext')}[{input_}, {output}]"
        elif self.grpc_type == GRPCType.SYNC:
            return self._import("grpc", "ServicerContext")
        else:
            # BOTH mode uses _ServicerContext union class
            return "_ServicerContext"

    def write_grpc_servicer_context(self) -> None:
        """Write _ServicerContext class only for BOTH mode (union type needed)."""
        wl = self._write_line

        if self.grpc_type != GRPCType.BOTH:
            return

        # BOTH mode: _ServicerContext is a union class that's supertyped by both
        # grpc.ServicerContext and grpc.aio.ServicerContext, so both can be used
        # in the contravariant function parameter position.
        wl(
            "class _ServicerContext({}, {}):  # type: ignore[misc, type-arg]",
            self._import("grpc", "ServicerContext"),
            self._import("grpc.aio", "ServicerContext"),
        )
        with self._indent():
            wl("...")
        wl("")

    def write_grpc_stub_methods(self, service: d.ServiceDescriptorProto, scl_prefix: SourceCodeLocation, *, is_async: bool, both: bool = False, ignore_type_error: bool = False) -> None:
        wl = self._write_line
        methods = [(i, m) for i, m in enumerate(service.method) if m.name not in PYTHON_RESERVED]
        if not methods:
            return

        def type_str(method: d.MethodDescriptorProto, is_async: bool) -> str:
            return f"{self._callable_type(method, is_async=is_async)}[{self._input_type(method)}, {self._output_type(method)}]"

        for i, method in methods:
            scl = scl_prefix + [d.ServiceDescriptorProto.METHOD_FIELD_NUMBER, i]
            is_deprecated = method.options.deprecated
            has_comments = self._has_comments(scl)

            # Generate type annotation once
            if both:
                type_annotation = f"{self._import('typing', 'Union')}[{type_str(method, is_async=False)}, {type_str(method, is_async=True)}]"
            else:
                type_annotation = type_str(method, is_async=is_async)

            if is_deprecated:
                wl(self._property())
                self._write_deprecation_warning(
                    scl + [d.MethodDescriptorProto.OPTIONS_FIELD_NUMBER, d.MethodOptions.DEPRECATED_FIELD_NUMBER],
                    "This method has been marked as deprecated using proto method options.",
                )
                wl(f"def {method.name}(self) -> {type_annotation}:{' ...' if not has_comments else ''}{'  # type: ignore[override]' if ignore_type_error else ''}")

                if has_comments:
                    with self._indent():
                        if not self._write_comments(scl):
                            wl("...")
            else:
                wl(f"{method.name}: {type_annotation}{'  # type: ignore[assignment]' if ignore_type_error else ''}")
                self._write_comments(scl)

    def write_grpc_methods(self, service: d.ServiceDescriptorProto, scl_prefix: SourceCodeLocation) -> None:
        wl = self._write_line
        methods = [(i, m) for i, m in enumerate(service.method) if m.name not in PYTHON_RESERVED]
        if not methods:
            wl("...")
            wl("")
        for i, method in methods:
            scl = scl_prefix + [d.ServiceDescriptorProto.METHOD_FIELD_NUMBER, i]
            input_type = self._servicer_input_type(method)
            output_type = self._servicer_output_type(method)

            if method.options.deprecated:
                self._write_deprecation_warning(scl, "This method has been marked as deprecated using proto method options.")
            if self.generate_concrete_servicer_stubs is False:
                wl("@{}", self._import("abc", "abstractmethod"))
            wl("def {}(", method.name)
            with self._indent():
                wl("self,")
                input_name = "request_iterator" if method.client_streaming else "request"
                wl(f"{input_name}: {input_type},")
                wl("context: {},", self.get_servicer_context_type(input_type, output_type))
            wl(
                ") -> {}:{}",
                output_type,
                " ..." if not self._has_comments(scl) else "",
            )
            if self._has_comments(scl):
                with self._indent():
                    if not self._write_comments(scl):
                        wl("...")
            wl("")

    def make_server_type(self) -> str:
        # Don't call the _import call yet, otherwise we get unnecessary imports
        def server() -> str:
            return self._import("grpc", "Server")

        def aserver() -> str:
            return self._import("grpc.aio", "Server")

        if self.grpc_type == GRPCType.BOTH:
            return f"{self._import('typing', 'Union')}[{server()}, {aserver()}]"
        elif self.grpc_type == GRPCType.ASYNC:
            return aserver()
        elif self.grpc_type == GRPCType.SYNC:
            return server()
        else:
            raise RuntimeError(f"Impossible, {self.grpc_type=}")

    def write_grpc_services(
        self,
        services: Iterable[d.ServiceDescriptorProto],
        scl_prefix: SourceCodeLocation,
    ) -> None:
        wl = self._write_line
        wl("GRPC_GENERATED_VERSION: str")
        wl("GRPC_VERSION: str")
        wl("")
        for i, service in enumerate(services):
            if service.name in PYTHON_RESERVED:
                continue

            scl = scl_prefix + [i]

            class_name = f"{service.name}Stub"
            async_class_alias = f"{service.name}AsyncStub"

            # The stub client
            if self.grpc_type.supports_sync:
                if service.options.deprecated:
                    self._write_deprecation_warning(
                        scl + [d.ServiceDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.ServiceOptions.DEPRECATED_FIELD_NUMBER],
                        "This stub has been marked as deprecated using proto service options.",
                    )
                wl(
                    "class {}:",
                    class_name,
                )
                with self._indent():
                    if self._write_comments(scl):
                        wl("")

                    if self.grpc_type == GRPCType.BOTH:
                        # Write sync overload
                        wl("@{}", self._import("typing", "overload"))
                        wl(
                            "def __new__(cls, channel: {}) -> {}: ...",
                            self._import("grpc", "Channel"),
                            self._import("typing_extensions", "Self"),
                        )

                        # Write async overload
                        wl("@{}", self._import("typing", "overload"))
                        wl(
                            "def __new__(cls, channel: {}) -> {}: ...",
                            self._import("grpc.aio", "Channel"),
                            async_class_alias,
                        )
                    else:
                        # SYNC only - simple __init__
                        wl("def __init__(self, channel: {}) -> None: ...", self._import("grpc", "Channel"))

                    self.write_grpc_stub_methods(service, scl, is_async=False)
                    wl("")

            # Write AsyncStub
            if self.grpc_type.supports_async:
                if service.options.deprecated:
                    self._write_deprecation_warning(
                        scl + [d.ServiceDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.ServiceOptions.DEPRECATED_FIELD_NUMBER],
                        "This stub has been marked as deprecated using proto service options.",
                    )

                if self.grpc_type == GRPCType.BOTH:
                    # BOTH mode: AsyncStub inherits from Stub
                    wl("@{}", self._import("typing", "type_check_only"))
                    wl("class {}({}):", async_class_alias, class_name)
                    with self._indent():
                        if self._write_comments(scl):
                            wl("")
                        wl("def __init__(self, channel: {}) -> None: ...", self._import("grpc.aio", "Channel"))
                        self.write_grpc_stub_methods(service, scl, is_async=True, ignore_type_error=True)
                else:
                    # ASYNC only - use Stub name (not AsyncStub) since there's only one type
                    wl("class {}:", class_name)
                    with self._indent():
                        if self._write_comments(scl):
                            wl("")
                        wl("def __init__(self, channel: {}) -> None: ...", self._import("grpc.aio", "Channel"))
                        self.write_grpc_stub_methods(service, scl, is_async=True)
                wl("")

            # The service definition interface
            if service.options.deprecated:
                self._write_deprecation_warning(
                    scl + [d.ServiceDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.ServiceOptions.DEPRECATED_FIELD_NUMBER],
                    "This servicer has been marked as deprecated using proto service options.",
                )
            if self.generate_concrete_servicer_stubs is False:
                wl(
                    "class {}Servicer(metaclass={}):",
                    service.name,
                    self._import("abc", "ABCMeta"),
                )
            else:
                wl(
                    "class {}Servicer:",
                    service.name,
                )
            with self._indent():
                if self._write_comments(scl):
                    wl("")
                self.write_grpc_methods(service, scl)
            if service.options.deprecated:
                self._write_deprecation_warning(
                    scl + [d.ServiceDescriptorProto.OPTIONS_FIELD_NUMBER] + [d.ServiceOptions.DEPRECATED_FIELD_NUMBER],
                    "This servicer has been marked as deprecated using proto service options.",
                )

            wl(
                "def add_{}Servicer_to_server(servicer: {}Servicer, server: {}) -> None: ...",
                service.name,
                service.name,
                self.make_server_type(),
            )
            wl("")

    def python_type(self, field: d.FieldDescriptorProto, generic_container: bool = False) -> str:
        """
        generic_container
          if set, type the field with generic interfaces. Eg.
          - Iterable[int] rather than RepeatedScalarFieldContainer[int]
          - Mapping[k, v] rather than MessageMap[k, v]
          Can be useful for input types (eg constructor)
        """
        oldstyle_casttype = field.options.Extensions[extensions_pb2.casttype]
        if oldstyle_casttype:
            print(f"Warning: Field {field.name}: (mypy_protobuf.casttype) is deprecated. Prefer (mypy_protobuf.options).casttype", file=sys.stderr)
        casttype = field.options.Extensions[extensions_pb2.options].casttype or oldstyle_casttype
        if casttype:
            return self._import_casttype(casttype)

        mapping: Dict[d.FieldDescriptorProto.Type.V, Callable[[], str]] = {
            d.FieldDescriptorProto.TYPE_DOUBLE: lambda: self._builtin("float"),
            d.FieldDescriptorProto.TYPE_FLOAT: lambda: self._builtin("float"),
            d.FieldDescriptorProto.TYPE_INT64: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_UINT64: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_FIXED64: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_SFIXED64: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_SINT64: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_INT32: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_UINT32: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_FIXED32: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_SFIXED32: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_SINT32: lambda: self._builtin("int"),
            d.FieldDescriptorProto.TYPE_BOOL: lambda: self._builtin("bool"),
            d.FieldDescriptorProto.TYPE_STRING: lambda: self._builtin("str"),
            d.FieldDescriptorProto.TYPE_BYTES: lambda: self._builtin("bytes"),
            d.FieldDescriptorProto.TYPE_ENUM: lambda: self._import_message(field.type_name + ".ValueType"),
            d.FieldDescriptorProto.TYPE_MESSAGE: lambda: self._import_message(field.type_name),
            d.FieldDescriptorProto.TYPE_GROUP: lambda: self._import_message(field.type_name),
        }

        assert field.type in mapping, "Unrecognized type: " + repr(field.type)
        field_type = mapping[field.type]()

        # For non-repeated fields, we're done!
        if field.label != d.FieldDescriptorProto.LABEL_REPEATED:
            return field_type

        # Scalar repeated fields go in RepeatedScalarFieldContainer
        if is_scalar(field):
            container = (
                self._import("collections.abc", "Iterable")
                if generic_container
                else self._import(
                    "google.protobuf.internal.containers",
                    "RepeatedScalarFieldContainer",
                )
            )
            return f"{container}[{field_type}]"

        # non-scalar repeated map fields go in ScalarMap/MessageMap
        msg = self.descriptors.messages[field.type_name]
        if msg.options.map_entry:
            # map generates a special Entry wrapper message
            if generic_container:
                container = self._import("collections.abc", "Mapping")
            elif is_scalar(msg.field[1]):
                container = self._import("google.protobuf.internal.containers", "ScalarMap")
            else:
                container = self._import("google.protobuf.internal.containers", "MessageMap")
            ktype, vtype = self._map_key_value_types(field, msg.field[0], msg.field[1])
            return f"{container}[{ktype}, {vtype}]"

        # non-scalar repetated fields go in RepeatedCompositeFieldContainer
        container = (
            self._import("collections.abc", "Iterable")
            if generic_container
            else self._import(
                "google.protobuf.internal.containers",
                "RepeatedCompositeFieldContainer",
            )
        )
        return f"{container}[{field_type}]"

    def write(self) -> str:
        # save current module content, so that imports and module docstring can be inserted
        saved_lines = self.lines
        self.lines = []

        # module docstring may exist as comment before syntax (optional) or package name
        if not self._write_comments([d.FileDescriptorProto.PACKAGE_FIELD_NUMBER]):
            self._write_comments([d.FileDescriptorProto.SYNTAX_FIELD_NUMBER])

        if self.lines:
            assert self.lines[0].startswith('"""')
            self.lines[0] = f'"""{HEADER}{self.lines[0][3:]}'
            self._write_line("")
        else:
            self._write_line(f'"""{HEADER}"""\n')

        for reexport_idx in self.fd.public_dependency:
            reexport_file = self.fd.dependency[reexport_idx]
            reexport_fd = self.descriptors.files[reexport_file]
            reexport_imp = reexport_file[:-6].replace("-", "_").replace("/", ".") + "_pb2"
            names = [m.name for m in reexport_fd.message_type] + [m.name for m in reexport_fd.enum_type] + [v.name for m in reexport_fd.enum_type for v in m.value] + [m.name for m in reexport_fd.extension]

            if names:
                # Add the rexported imports as individual imports to preserve names. Do not mangle
                [self.imports.add_import(f"{reexport_imp}.{n}", n, mangle=False) for n in names]

        if self.typing_extensions_min or self.deprecated_min:
            # Special case for `sys` as it is needed for version checks
            self.imports.add_import("sys")
        for statement in sorted(self.imports.statements):
            self._write_line(statement)
        if self.typing_extensions_min:
            self._write_line("")
            self._write_line(f"if sys.version_info >= {self.typing_extensions_min}:")
            self._write_line(f"    from typing import {', '.join(self.imports.typing_statements)}")
            self._write_line("else:")
            self._write_line(f"    from typing_extensions import {', '.join(self.imports.typing_statements)}")
        if self.deprecated_min:
            self._write_line("")
            self._write_line(f"if sys.version_info >= {self.deprecated_min}:")
            self._write_line(f"    from warnings import deprecated as {self._deprecated_name}")
            self._write_line("else:")
            self._write_line(f"    from typing_extensions import deprecated as {self._deprecated_name}")

        self._write_line("")

        # restore module content
        self.lines += saved_lines

        content = "\n".join(self.lines)
        if not content.endswith("\n"):
            content = content + "\n"
        return content


def is_scalar(fd: d.FieldDescriptorProto) -> bool:
    return not (fd.type == d.FieldDescriptorProto.TYPE_MESSAGE or fd.type == d.FieldDescriptorProto.TYPE_GROUP)


def generate_mypy_stubs(
    descriptors: Descriptors,
    response: plugin_pb2.CodeGeneratorResponse,
    quiet: bool,
    readable_stubs: bool,
    relax_strict_optional_primitives: bool,
    use_default_deprecation_warnings: bool,
    generate_concrete_servicer_stubs: bool,
) -> None:
    for name, fd in descriptors.to_generate.items():
        pkg_writer = PkgWriter(
            fd,
            descriptors,
            readable_stubs,
            relax_strict_optional_primitives,
            use_default_deprecation_warnings,
            generate_concrete_servicer_stubs,
            grpc=False,
        )

        pkg_writer.write_module_attributes()
        pkg_writer.write_enums(fd.enum_type, "", [d.FileDescriptorProto.ENUM_TYPE_FIELD_NUMBER])
        pkg_writer.write_messages(fd.message_type, "", [d.FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER], fd.options.features.field_presence)
        pkg_writer.write_extensions(fd.extension, [d.FileDescriptorProto.EXTENSION_FIELD_NUMBER])

        assert name == fd.name
        assert fd.name.endswith(".proto")
        output = response.file.add()
        output.name = fd.name[:-6].replace("-", "_").replace(".", "/") + "_pb2.pyi"
        output.content = pkg_writer.write()
        if not quiet:
            print("Writing mypy to", output.name, file=sys.stderr)


def generate_mypy_grpc_stubs(
    descriptors: Descriptors,
    response: plugin_pb2.CodeGeneratorResponse,
    quiet: bool,
    readable_stubs: bool,
    relax_strict_optional_primitives: bool,
    use_default_deprecation_warnings: bool,
    generate_concrete_servicer_stubs: bool,
    grpc_type: GRPCType,
) -> None:
    for name, fd in descriptors.to_generate.items():
        pkg_writer = PkgWriter(
            fd,
            descriptors,
            readable_stubs,
            relax_strict_optional_primitives,
            use_default_deprecation_warnings,
            generate_concrete_servicer_stubs,
            grpc=True,
            grpc_type=grpc_type,
        )
        pkg_writer.write_grpc_iterator_type()
        pkg_writer.write_grpc_servicer_context()
        pkg_writer.write_grpc_services(fd.service, [d.FileDescriptorProto.SERVICE_FIELD_NUMBER])

        assert name == fd.name
        assert fd.name.endswith(".proto")
        output = response.file.add()
        output.name = fd.name[:-6].replace("-", "_").replace(".", "/") + "_pb2_grpc.pyi"
        output.content = pkg_writer.write()
        if not quiet:
            print("Writing mypy to", output.name, file=sys.stderr)


@contextmanager
def code_generation() -> Iterator[Tuple[plugin_pb2.CodeGeneratorRequest, plugin_pb2.CodeGeneratorResponse],]:
    if len(sys.argv) > 1 and sys.argv[1] in ("-V", "--version"):
        print("mypy-protobuf " + __version__)
        sys.exit(0)

    # Read request message from stdin
    data = sys.stdin.buffer.read()

    # Parse request
    request = plugin_pb2.CodeGeneratorRequest()
    request.ParseFromString(data)

    # Create response
    response = plugin_pb2.CodeGeneratorResponse()

    # Declare support for optional proto3 fields
    response.supported_features |= plugin_pb2.CodeGeneratorResponse.FEATURE_PROTO3_OPTIONAL
    response.supported_features |= plugin_pb2.CodeGeneratorResponse.FEATURE_SUPPORTS_EDITIONS

    response.minimum_edition = d.EDITION_LEGACY
    response.maximum_edition = d.EDITION_2024

    yield request, response

    # Serialise response message
    output = response.SerializeToString()

    # Write to stdout
    sys.stdout.buffer.write(output)


def main() -> None:
    # Generate mypy
    with code_generation() as (request, response):
        generate_mypy_stubs(
            Descriptors(request),
            response,
            "quiet" in request.parameter,
            "readable_stubs" in request.parameter,
            "relax_strict_optional_primitives" in request.parameter,
            "use_default_deprecation_warnings" in request.parameter,
            "generate_concrete_servicer_stubs" in request.parameter,
        )


def grpc() -> None:
    # Generate grpc mypy
    with code_generation() as (request, response):
        generate_mypy_grpc_stubs(
            Descriptors(request),
            response,
            "quiet" in request.parameter,
            "readable_stubs" in request.parameter,
            "relax_strict_optional_primitives" in request.parameter,
            "use_default_deprecation_warnings" in request.parameter,
            "generate_concrete_servicer_stubs" in request.parameter,
            GRPCType.from_parameter(request.parameter),
        )


if __name__ == "__main__":
    main()
