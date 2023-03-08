// ignore_for_file: directives_ordering
// ignore_for_file: no_leading_underscores_for_library_prefixes
import 'package:build_runner_core/build_runner_core.dart' as _i1;
import 'package:enum_map_gen/builder.dart' as _i2;
import 'package:flutter_gen_runner/flutter_gen_runner.dart' as _i3;
import 'package:json_serializable/builder.dart' as _i4;
import 'package:mockito/src/builder.dart' as _i5;
import 'package:source_gen/builder.dart' as _i6;
import 'dart:isolate' as _i7;
import 'package:build_runner/build_runner.dart' as _i8;
import 'dart:io' as _i9;

final _builders = <_i1.BuilderApplication>[
  _i1.apply(
    r'enum_map_gen:enum_map',
    [
      _i2.modifiableEnumMapBuilder,
      _i2.unmodifiableEnumMapBuilder,
    ],
    _i1.toDependentsOf(r'enum_map_gen'),
    hideOutput: true,
    appliesBuilders: const [r'source_gen:combining_builder'],
  ),
  _i1.apply(
    r'flutter_gen_runner:flutter_gen_runner',
    [_i3.build],
    _i1.toDependentsOf(r'flutter_gen_runner'),
    hideOutput: false,
  ),
  _i1.apply(
    r'json_serializable:json_serializable',
    [_i4.jsonSerializable],
    _i1.toDependentsOf(r'json_serializable'),
    hideOutput: true,
    appliesBuilders: const [r'source_gen:combining_builder'],
  ),
  _i1.apply(
    r'mockito:mockBuilder',
    [_i5.buildMocks],
    _i1.toDependentsOf(r'mockito'),
    hideOutput: false,
  ),
  _i1.apply(
    r'source_gen:combining_builder',
    [_i6.combiningBuilder],
    _i1.toNoneByDefault(),
    hideOutput: false,
    appliesBuilders: const [r'source_gen:part_cleanup'],
  ),
  _i1.applyPostProcess(
    r'source_gen:part_cleanup',
    _i6.partCleanup,
  ),
];
void main(
  List<String> args, [
  _i7.SendPort? sendPort,
]) async {
  var result = await _i8.run(
    args,
    _builders,
  );
  sendPort?.send(result);
  _i9.exitCode = result;
}
