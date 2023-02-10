from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

COMPLEXITY_ADVANCED: Complexity
COMPLEXITY_BASIC: Complexity
COMPLEXITY_MEDIUM: Complexity
COMPLEXITY_UNSPECIFIED: Complexity
DESCRIPTOR: _descriptor.FileDescriptor
EMULATOR_TYPE_KAFKA: EmulatorType
EMULATOR_TYPE_UNSPECIFIED: EmulatorType
PRECOMPILED_OBJECT_TYPE_EXAMPLE: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_KATA: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_UNIT_TEST: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_UNSPECIFIED: PrecompiledObjectType
SDK_GO: Sdk
SDK_JAVA: Sdk
SDK_PYTHON: Sdk
SDK_SCIO: Sdk
SDK_UNSPECIFIED: Sdk
STATUS_CANCELED: Status
STATUS_COMPILE_ERROR: Status
STATUS_COMPILING: Status
STATUS_ERROR: Status
STATUS_EXECUTING: Status
STATUS_FINISHED: Status
STATUS_PREPARATION_ERROR: Status
STATUS_PREPARING: Status
STATUS_RUN_ERROR: Status
STATUS_RUN_TIMEOUT: Status
STATUS_UNSPECIFIED: Status
STATUS_VALIDATING: Status
STATUS_VALIDATION_ERROR: Status

class CancelRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class CancelResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Categories(_message.Message):
    __slots__ = ["categories", "sdk"]
    class Category(_message.Message):
        __slots__ = ["category_name", "precompiled_objects"]
        CATEGORY_NAME_FIELD_NUMBER: _ClassVar[int]
        PRECOMPILED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
        category_name: str
        precompiled_objects: _containers.RepeatedCompositeFieldContainer[PrecompiledObject]
        def __init__(self, category_name: _Optional[str] = ..., precompiled_objects: _Optional[_Iterable[_Union[PrecompiledObject, _Mapping]]] = ...) -> None: ...
    CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    categories: _containers.RepeatedCompositeFieldContainer[Categories.Category]
    sdk: Sdk
    def __init__(self, sdk: _Optional[_Union[Sdk, str]] = ..., categories: _Optional[_Iterable[_Union[Categories.Category, _Mapping]]] = ...) -> None: ...

class CheckStatusRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class CheckStatusResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: Status
    def __init__(self, status: _Optional[_Union[Status, str]] = ...) -> None: ...

class Dataset(_message.Message):
    __slots__ = ["dataset_path", "options", "type"]
    class OptionsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DATASET_PATH_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    dataset_path: str
    options: _containers.ScalarMap[str, str]
    type: EmulatorType
    def __init__(self, type: _Optional[_Union[EmulatorType, str]] = ..., options: _Optional[_Mapping[str, str]] = ..., dataset_path: _Optional[str] = ...) -> None: ...

class GetCompileOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetCompileOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetDefaultPrecompiledObjectRequest(_message.Message):
    __slots__ = ["sdk"]
    SDK_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    def __init__(self, sdk: _Optional[_Union[Sdk, str]] = ...) -> None: ...

class GetDefaultPrecompiledObjectResponse(_message.Message):
    __slots__ = ["precompiled_object"]
    PRECOMPILED_OBJECT_FIELD_NUMBER: _ClassVar[int]
    precompiled_object: PrecompiledObject
    def __init__(self, precompiled_object: _Optional[_Union[PrecompiledObject, _Mapping]] = ...) -> None: ...

class GetGraphRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetGraphResponse(_message.Message):
    __slots__ = ["graph"]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    graph: str
    def __init__(self, graph: _Optional[str] = ...) -> None: ...

class GetLogsRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetLogsResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectCodeRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectCodeResponse(_message.Message):
    __slots__ = ["code", "files"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    code: str
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    def __init__(self, code: _Optional[str] = ..., files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ...) -> None: ...

class GetPrecompiledObjectGraphRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectGraphResponse(_message.Message):
    __slots__ = ["graph"]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    graph: str
    def __init__(self, graph: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectLogsRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectLogsResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectOutputRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectResponse(_message.Message):
    __slots__ = ["precompiled_object"]
    PRECOMPILED_OBJECT_FIELD_NUMBER: _ClassVar[int]
    precompiled_object: PrecompiledObject
    def __init__(self, precompiled_object: _Optional[_Union[PrecompiledObject, _Mapping]] = ...) -> None: ...

class GetPrecompiledObjectsRequest(_message.Message):
    __slots__ = ["category", "sdk"]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    category: str
    sdk: Sdk
    def __init__(self, sdk: _Optional[_Union[Sdk, str]] = ..., category: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectsResponse(_message.Message):
    __slots__ = ["sdk_categories"]
    SDK_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    sdk_categories: _containers.RepeatedCompositeFieldContainer[Categories]
    def __init__(self, sdk_categories: _Optional[_Iterable[_Union[Categories, _Mapping]]] = ...) -> None: ...

class GetPreparationOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetPreparationOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetRunErrorRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetRunErrorResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetRunOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetRunOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetSnippetRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetSnippetResponse(_message.Message):
    __slots__ = ["complexity", "files", "pipeline_options", "sdk"]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    complexity: Complexity
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    pipeline_options: str
    sdk: Sdk
    def __init__(self, files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ..., sdk: _Optional[_Union[Sdk, str]] = ..., pipeline_options: _Optional[str] = ..., complexity: _Optional[_Union[Complexity, str]] = ...) -> None: ...

class GetValidationOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class GetValidationOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class PrecompiledObject(_message.Message):
    __slots__ = ["cloud_path", "complexity", "context_line", "datasets", "default_example", "description", "link", "multifile", "name", "pipeline_options", "sdk", "tags", "type", "url_notebook", "url_vcs"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_LINE_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_EXAMPLE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    LINK_FIELD_NUMBER: _ClassVar[int]
    MULTIFILE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    URL_NOTEBOOK_FIELD_NUMBER: _ClassVar[int]
    URL_VCS_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    complexity: Complexity
    context_line: int
    datasets: _containers.RepeatedCompositeFieldContainer[Dataset]
    default_example: bool
    description: str
    link: str
    multifile: bool
    name: str
    pipeline_options: str
    sdk: Sdk
    tags: _containers.RepeatedScalarFieldContainer[str]
    type: PrecompiledObjectType
    url_notebook: str
    url_vcs: str
    def __init__(self, cloud_path: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., type: _Optional[_Union[PrecompiledObjectType, str]] = ..., pipeline_options: _Optional[str] = ..., link: _Optional[str] = ..., multifile: bool = ..., context_line: _Optional[int] = ..., default_example: bool = ..., sdk: _Optional[_Union[Sdk, str]] = ..., complexity: _Optional[_Union[Complexity, str]] = ..., tags: _Optional[_Iterable[str]] = ..., datasets: _Optional[_Iterable[_Union[Dataset, _Mapping]]] = ..., url_vcs: _Optional[str] = ..., url_notebook: _Optional[str] = ...) -> None: ...

class RunCodeRequest(_message.Message):
    __slots__ = ["code", "datasets", "files", "pipeline_options", "sdk"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    code: str
    datasets: _containers.RepeatedCompositeFieldContainer[Dataset]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    pipeline_options: str
    sdk: Sdk
    def __init__(self, code: _Optional[str] = ..., sdk: _Optional[_Union[Sdk, str]] = ..., pipeline_options: _Optional[str] = ..., datasets: _Optional[_Iterable[_Union[Dataset, _Mapping]]] = ..., files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ...) -> None: ...

class RunCodeResponse(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class SaveSnippetRequest(_message.Message):
    __slots__ = ["complexity", "files", "persistence_key", "pipeline_options", "sdk"]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    PERSISTENCE_KEY_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    complexity: Complexity
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    persistence_key: str
    pipeline_options: str
    sdk: Sdk
    def __init__(self, files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ..., sdk: _Optional[_Union[Sdk, str]] = ..., pipeline_options: _Optional[str] = ..., complexity: _Optional[_Union[Complexity, str]] = ..., persistence_key: _Optional[str] = ...) -> None: ...

class SaveSnippetResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class SnippetFile(_message.Message):
    __slots__ = ["content", "is_main", "name"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    IS_MAIN_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    content: str
    is_main: bool
    name: str
    def __init__(self, name: _Optional[str] = ..., content: _Optional[str] = ..., is_main: bool = ...) -> None: ...

class Sdk(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class PrecompiledObjectType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Complexity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class EmulatorType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
