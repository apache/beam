// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'output_tab.dart';

// **************************************************************************
// UnmodifiableEnumMapGenerator
// **************************************************************************

class UnmodifiableOutputTabEnumMap<V>
    extends UnmodifiableEnumMap<OutputTabEnum, V> {
  final V result;
  final V graph;

  const UnmodifiableOutputTabEnumMap({
    required this.result,
    required this.graph,
  });

  @override
  Map<RK, RV> cast<RK, RV>() {
    return Map.castFrom<OutputTabEnum, V, RK, RV>(this);
  }

  @override
  bool containsValue(Object? value) {
    if (this.result == value) return true;
    if (this.graph == value) return true;
    return false;
  }

  @override
  bool containsKey(Object? key) {
    return key.runtimeType == OutputTabEnum;
  }

  @override
  V? operator [](Object? key) {
    switch (key) {
      case OutputTabEnum.result:
        return this.result;
      case OutputTabEnum.graph:
        return this.graph;
    }

    return null;
  }

  @override
  void operator []=(OutputTabEnum key, V value) {
    throw Exception("Cannot modify this map.");
  }

  @override
  Iterable<MapEntry<OutputTabEnum, V>> get entries {
    return [
      MapEntry<OutputTabEnum, V>(OutputTabEnum.result, this.result),
      MapEntry<OutputTabEnum, V>(OutputTabEnum.graph, this.graph),
    ];
  }

  @override
  Map<K2, V2> map<K2, V2>(
      MapEntry<K2, V2> transform(OutputTabEnum key, V value)) {
    final result = transform(OutputTabEnum.result, this.result);
    final graph = transform(OutputTabEnum.graph, this.graph);
    return {
      result.key: result.value,
      graph.key: graph.value,
    };
  }

  @override
  void addEntries(Iterable<MapEntry<OutputTabEnum, V>> newEntries) {
    throw Exception("Cannot modify this map.");
  }

  @override
  V update(OutputTabEnum key, V update(V value), {V Function()? ifAbsent}) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void updateAll(V update(OutputTabEnum key, V value)) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void removeWhere(bool test(OutputTabEnum key, V value)) {
    throw Exception("Objects in this map cannot be removed.");
  }

  @override
  V putIfAbsent(OutputTabEnum key, V ifAbsent()) {
    return this.get(key);
  }

  @override
  void addAll(Map<OutputTabEnum, V> other) {
    throw Exception("Cannot modify this map.");
  }

  @override
  V? remove(Object? key) {
    throw Exception("Objects in this map cannot be removed.");
  }

  @override
  void clear() {
    throw Exception("Objects in this map cannot be removed.");
  }

  @override
  void forEach(void action(OutputTabEnum key, V value)) {
    action(OutputTabEnum.result, this.result);
    action(OutputTabEnum.graph, this.graph);
  }

  @override
  Iterable<OutputTabEnum> get keys {
    return OutputTabEnum.values;
  }

  @override
  Iterable<V> get values {
    return [
      this.result,
      this.graph,
    ];
  }

  @override
  int get length {
    return 2;
  }

  @override
  bool get isEmpty {
    return false;
  }

  @override
  bool get isNotEmpty {
    return true;
  }

  V get(OutputTabEnum key) {
    switch (key) {
      case OutputTabEnum.result:
        return this.result;
      case OutputTabEnum.graph:
        return this.graph;
    }
  }

  @override
  String toString() {
    final buffer = StringBuffer("{");
    buffer.write("OutputTabEnum.result: ${this.result}");
    buffer.write(", ");
    buffer.write("OutputTabEnum.graph: ${this.graph}");
    buffer.write("}");
    return buffer.toString();
  }
}
