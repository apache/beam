from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Sdk(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    SDK_UNSPECIFIED: _ClassVar[Sdk]
    SDK_JAVA: _ClassVar[Sdk]
    SDK_GO: _ClassVar[Sdk]
    SDK_PYTHON: _ClassVar[Sdk]
    SDK_SCIO: _ClassVar[Sdk]

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    STATUS_UNSPECIFIED: _ClassVar[Status]
    STATUS_VALIDATING: _ClassVar[Status]
    STATUS_VALIDATION_ERROR: _ClassVar[Status]
    STATUS_PREPARING: _ClassVar[Status]
    STATUS_PREPARATION_ERROR: _ClassVar[Status]
    STATUS_COMPILING: _ClassVar[Status]
    STATUS_COMPILE_ERROR: _ClassVar[Status]
    STATUS_EXECUTING: _ClassVar[Status]
    STATUS_FINISHED: _ClassVar[Status]
    STATUS_RUN_ERROR: _ClassVar[Status]
    STATUS_ERROR: _ClassVar[Status]
    STATUS_RUN_TIMEOUT: _ClassVar[Status]
    STATUS_CANCELED: _ClassVar[Status]

class PrecompiledObjectType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    PRECOMPILED_OBJECT_TYPE_UNSPECIFIED: _ClassVar[PrecompiledObjectType]
    PRECOMPILED_OBJECT_TYPE_EXAMPLE: _ClassVar[PrecompiledObjectType]
    PRECOMPILED_OBJECT_TYPE_KATA: _ClassVar[PrecompiledObjectType]
    PRECOMPILED_OBJECT_TYPE_UNIT_TEST: _ClassVar[PrecompiledObjectType]

class Complexity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    COMPLEXITY_UNSPECIFIED: _ClassVar[Complexity]
    COMPLEXITY_BASIC: _ClassVar[Complexity]
    COMPLEXITY_MEDIUM: _ClassVar[Complexity]
    COMPLEXITY_ADVANCED: _ClassVar[Complexity]

class EmulatorType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    EMULATOR_TYPE_UNSPECIFIED: _ClassVar[EmulatorType]
    EMULATOR_TYPE_KAFKA: _ClassVar[EmulatorType]
SDK_UNSPECIFIED: Sdk
SDK_JAVA: Sdk
SDK_GO: Sdk
SDK_PYTHON: Sdk
SDK_SCIO: Sdk
STATUS_UNSPECIFIED: Status
STATUS_VALIDATING: Status
STATUS_VALIDATION_ERROR: Status
STATUS_PREPARING: Status
STATUS_PREPARATION_ERROR: Status
STATUS_COMPILING: Status
STATUS_COMPILE_ERROR: Status
STATUS_EXECUTING: Status
STATUS_FINISHED: Status
STATUS_RUN_ERROR: Status
STATUS_ERROR: Status
STATUS_RUN_TIMEOUT: Status
STATUS_CANCELED: Status
PRECOMPILED_OBJECT_TYPE_UNSPECIFIED: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_EXAMPLE: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_KATA: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_UNIT_TEST: PrecompiledObjectType
COMPLEXITY_UNSPECIFIED: Complexity
COMPLEXITY_BASIC: Complexity
COMPLEXITY_MEDIUM: Complexity
COMPLEXITY_ADVANCED: Complexity
EMULATOR_TYPE_UNSPECIFIED: EmulatorType
EMULATOR_TYPE_KAFKA: EmulatorType

class Dataset(_message.Message):
    __slots__ = ["type", "options", "dataset_path"]
    class OptionsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TYPE_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATASET_PATH_FIELD_NUMBER: _ClassVar[int]
    type: EmulatorType
    options: _containers.ScalarMap[str, str]
    dataset_path: str
    def __init__(self, type: _Optional[_Union[EmulatorType, str]] = ..., options: _Optional[_Mapping[str, str]] = ..., dataset_path: _Optional[str] = ...) -> None: ...

class RunCodeRequest(_message.Message):
    __slots__ = ["code", "sdk", "pipeline_options", "datasets", "files"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    code: str
    sdk: Sdk
    pipeline_options: str
    datasets: _containers.RepeatedCompositeFieldContainer[Dataset]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    def __init__(self, code: _Optional[str] = ..., sdk: _Optional[_Union[Sdk, str]] = ..., pipeline_options: _Optional[str] = ..., datasets: _Optional[_Iterable[_Union[Dataset, _Mapping]]] = ..., files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ...) -> None: ...

class RunCodeResponse(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

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

class CancelRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: _Optional[str] = ...) -> None: ...

class CancelResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class PrecompiledObject(_message.Message):
    __slots__ = ["cloud_path", "name", "description", "type", "pipeline_options", "link", "multifile", "context_line", "default_example", "sdk", "complexity", "tags", "datasets", "url_vcs", "url_notebook", "always_run", "never_run"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    LINK_FIELD_NUMBER: _ClassVar[int]
    MULTIFILE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_LINE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_EXAMPLE_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    URL_VCS_FIELD_NUMBER: _ClassVar[int]
    URL_NOTEBOOK_FIELD_NUMBER: _ClassVar[int]
    ALWAYS_RUN_FIELD_NUMBER: _ClassVar[int]
    NEVER_RUN_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    name: str
    description: str
    type: PrecompiledObjectType
    pipeline_options: str
    link: str
    multifile: bool
    context_line: int
    default_example: bool
    sdk: Sdk
    complexity: Complexity
    tags: _containers.RepeatedScalarFieldContainer[str]
    datasets: _containers.RepeatedCompositeFieldContainer[Dataset]
    url_vcs: str
    url_notebook: str
    always_run: bool
    never_run: bool
    def __init__(self, cloud_path: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., type: _Optional[_Union[PrecompiledObjectType, str]] = ..., pipeline_options: _Optional[str] = ..., link: _Optional[str] = ..., multifile: bool = ..., context_line: _Optional[int] = ..., default_example: bool = ..., sdk: _Optional[_Union[Sdk, str]] = ..., complexity: _Optional[_Union[Complexity, str]] = ..., tags: _Optional[_Iterable[str]] = ..., datasets: _Optional[_Iterable[_Union[Dataset, _Mapping]]] = ..., url_vcs: _Optional[str] = ..., url_notebook: _Optional[str] = ..., always_run: bool = ..., never_run: bool = ...) -> None: ...

class Categories(_message.Message):
    __slots__ = ["sdk", "categories"]
    class Category(_message.Message):
        __slots__ = ["category_name", "precompiled_objects"]
        CATEGORY_NAME_FIELD_NUMBER: _ClassVar[int]
        PRECOMPILED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
        category_name: str
        precompiled_objects: _containers.RepeatedCompositeFieldContainer[PrecompiledObject]
        def __init__(self, category_name: _Optional[str] = ..., precompiled_objects: _Optional[_Iterable[_Union[PrecompiledObject, _Mapping]]] = ...) -> None: ...
    SDK_FIELD_NUMBER: _ClassVar[int]
    CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    categories: _containers.RepeatedCompositeFieldContainer[Categories.Category]
    def __init__(self, sdk: _Optional[_Union[Sdk, str]] = ..., categories: _Optional[_Iterable[_Union[Categories.Category, _Mapping]]] = ...) -> None: ...

class GetPrecompiledObjectsRequest(_message.Message):
    __slots__ = ["sdk", "category"]
    SDK_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    category: str
    def __init__(self, sdk: _Optional[_Union[Sdk, str]] = ..., category: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectCodeRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectOutputRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectLogsRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectGraphRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: _Optional[str] = ...) -> None: ...

class GetDefaultPrecompiledObjectRequest(_message.Message):
    __slots__ = ["sdk"]
    SDK_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    def __init__(self, sdk: _Optional[_Union[Sdk, str]] = ...) -> None: ...

class GetPrecompiledObjectsResponse(_message.Message):
    __slots__ = ["sdk_categories"]
    SDK_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    sdk_categories: _containers.RepeatedCompositeFieldContainer[Categories]
    def __init__(self, sdk_categories: _Optional[_Iterable[_Union[Categories, _Mapping]]] = ...) -> None: ...

class GetPrecompiledObjectResponse(_message.Message):
    __slots__ = ["precompiled_object"]
    PRECOMPILED_OBJECT_FIELD_NUMBER: _ClassVar[int]
    precompiled_object: PrecompiledObject
    def __init__(self, precompiled_object: _Optional[_Union[PrecompiledObject, _Mapping]] = ...) -> None: ...

class GetPrecompiledObjectCodeResponse(_message.Message):
    __slots__ = ["code", "files"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    code: str
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    def __init__(self, code: _Optional[str] = ..., files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ...) -> None: ...

class GetPrecompiledObjectOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectLogsResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: _Optional[str] = ...) -> None: ...

class GetPrecompiledObjectGraphResponse(_message.Message):
    __slots__ = ["graph"]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    graph: str
    def __init__(self, graph: _Optional[str] = ...) -> None: ...

class GetDefaultPrecompiledObjectResponse(_message.Message):
    __slots__ = ["precompiled_object"]
    PRECOMPILED_OBJECT_FIELD_NUMBER: _ClassVar[int]
    precompiled_object: PrecompiledObject
    def __init__(self, precompiled_object: _Optional[_Union[PrecompiledObject, _Mapping]] = ...) -> None: ...

class SnippetFile(_message.Message):
    __slots__ = ["name", "content", "is_main"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    IS_MAIN_FIELD_NUMBER: _ClassVar[int]
    name: str
    content: str
    is_main: bool
    def __init__(self, name: _Optional[str] = ..., content: _Optional[str] = ..., is_main: bool = ...) -> None: ...

class SaveSnippetRequest(_message.Message):
    __slots__ = ["files", "sdk", "pipeline_options", "complexity", "persistence_key"]
    FILES_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    PERSISTENCE_KEY_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    sdk: Sdk
    pipeline_options: str
    complexity: Complexity
    persistence_key: str
    def __init__(self, files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ..., sdk: _Optional[_Union[Sdk, str]] = ..., pipeline_options: _Optional[str] = ..., complexity: _Optional[_Union[Complexity, str]] = ..., persistence_key: _Optional[str] = ...) -> None: ...

class SaveSnippetResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetSnippetRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetSnippetResponse(_message.Message):
    __slots__ = ["files", "sdk", "pipeline_options", "complexity"]
    FILES_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    sdk: Sdk
    pipeline_options: str
    complexity: Complexity
    def __init__(self, files: _Optional[_Iterable[_Union[SnippetFile, _Mapping]]] = ..., sdk: _Optional[_Union[Sdk, str]] = ..., pipeline_options: _Optional[str] = ..., complexity: _Optional[_Union[Complexity, str]] = ...) -> None: ...

class GetMetadataRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetMetadataResponse(_message.Message):
    __slots__ = ["runner_sdk", "build_commit_hash", "build_commit_timestamp_seconds_since_epoch", "beam_sdk_version"]
    RUNNER_SDK_FIELD_NUMBER: _ClassVar[int]
    BUILD_COMMIT_HASH_FIELD_NUMBER: _ClassVar[int]
    BUILD_COMMIT_TIMESTAMP_SECONDS_SINCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    BEAM_SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    runner_sdk: str
    build_commit_hash: str
    build_commit_timestamp_seconds_since_epoch: int
    beam_sdk_version: str
    def __init__(self, runner_sdk: _Optional[str] = ..., build_commit_hash: _Optional[str] = ..., build_commit_timestamp_seconds_since_epoch: _Optional[int] = ..., beam_sdk_version: _Optional[str] = ...) -> None: ...
