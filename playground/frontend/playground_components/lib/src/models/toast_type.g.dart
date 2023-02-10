// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'toast_type.dart';

// **************************************************************************
// UnmodifiableEnumMapGenerator
// **************************************************************************

class UnmodifiableToastTypeMap<V> extends UnmodifiableEnumMap<ToastType, V> {
  final V error;
  final V info;

  const UnmodifiableToastTypeMap({
    required this.error,
    required this.info,
  });

  @override
  Map<RK, RV> cast<RK, RV>() {
    return Map.castFrom<ToastType, V, RK, RV>(this);
  }

  @override
  bool containsValue(Object? value) {
    if (this.error == value) return true;
    if (this.info == value) return true;
    return false;
  }

  @override
  bool containsKey(Object? key) {
    return key.runtimeType == ToastType;
  }

  @override
  V? operator [](Object? key) {
    switch (key) {
      case ToastType.error:
        return this.error;
      case ToastType.info:
        return this.info;
    }

    return null;
  }

  @override
  void operator []=(ToastType key, V value) {
    throw Exception("Cannot modify this map.");
  }

  @override
  Iterable<MapEntry<ToastType, V>> get entries {
    return [
      MapEntry<ToastType, V>(ToastType.error, this.error),
      MapEntry<ToastType, V>(ToastType.info, this.info),
    ];
  }

  @override
  Map<K2, V2> map<K2, V2>(MapEntry<K2, V2> transform(ToastType key, V value)) {
    final error = transform(ToastType.error, this.error);
    final info = transform(ToastType.info, this.info);
    return {
      error.key: error.value,
      info.key: info.value,
    };
  }

  @override
  void addEntries(Iterable<MapEntry<ToastType, V>> newEntries) {
    throw Exception("Cannot modify this map.");
  }

  @override
  V update(ToastType key, V update(V value), {V Function()? ifAbsent}) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void updateAll(V update(ToastType key, V value)) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void removeWhere(bool test(ToastType key, V value)) {
    throw Exception("Objects in this map cannot be removed.");
  }

  @override
  V putIfAbsent(ToastType key, V ifAbsent()) {
    return this.get(key);
  }

  @override
  void addAll(Map<ToastType, V> other) {
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
  void forEach(void action(ToastType key, V value)) {
    action(ToastType.error, this.error);
    action(ToastType.info, this.info);
  }

  @override
  Iterable<ToastType> get keys {
    return ToastType.values;
  }

  @override
  Iterable<V> get values {
    return [
      this.error,
      this.info,
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

  V get(ToastType key) {
    switch (key) {
      case ToastType.error:
        return this.error;
      case ToastType.info:
        return this.info;
    }
  }

  @override
  String toString() {
    final buffer = StringBuffer("{");
    buffer.write("ToastType.error: ${this.error}");
    buffer.write(", ");
    buffer.write("ToastType.info: ${this.info}");
    buffer.write("}");
    return buffer.toString();
  }
}
