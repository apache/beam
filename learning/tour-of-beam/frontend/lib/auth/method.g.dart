// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'method.dart';

// **************************************************************************
// UnmodifiableEnumMapGenerator
// **************************************************************************

class UnmodifiableAuthMethodMap<V> extends UnmodifiableEnumMap<AuthMethod, V> {
  final V google;
  final V github;

  const UnmodifiableAuthMethodMap({
    required this.google,
    required this.github,
  });

  @override
  Map<RK, RV> cast<RK, RV>() {
    return Map.castFrom<AuthMethod, V, RK, RV>(this);
  }

  @override
  bool containsValue(Object? value) {
    if (this.google == value) return true;
    if (this.github == value) return true;
    return false;
  }

  @override
  bool containsKey(Object? key) {
    return key.runtimeType == AuthMethod;
  }

  @override
  V? operator [](Object? key) {
    switch (key) {
      case AuthMethod.google:
        return this.google;
      case AuthMethod.github:
        return this.github;
    }

    return null;
  }

  @override
  void operator []=(AuthMethod key, V value) {
    throw Exception("Cannot modify this map.");
  }

  @override
  Iterable<MapEntry<AuthMethod, V>> get entries {
    return [
      MapEntry<AuthMethod, V>(AuthMethod.google, this.google),
      MapEntry<AuthMethod, V>(AuthMethod.github, this.github),
    ];
  }

  @override
  Map<K2, V2> map<K2, V2>(MapEntry<K2, V2> transform(AuthMethod key, V value)) {
    final google = transform(AuthMethod.google, this.google);
    final github = transform(AuthMethod.github, this.github);
    return {
      google.key: google.value,
      github.key: github.value,
    };
  }

  @override
  void addEntries(Iterable<MapEntry<AuthMethod, V>> newEntries) {
    throw Exception("Cannot modify this map.");
  }

  @override
  V update(AuthMethod key, V update(V value), {V Function()? ifAbsent}) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void updateAll(V update(AuthMethod key, V value)) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void removeWhere(bool test(AuthMethod key, V value)) {
    throw Exception("Objects in this map cannot be removed.");
  }

  @override
  V putIfAbsent(AuthMethod key, V ifAbsent()) {
    return this.get(key);
  }

  @override
  void addAll(Map<AuthMethod, V> other) {
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
  void forEach(void action(AuthMethod key, V value)) {
    action(AuthMethod.google, this.google);
    action(AuthMethod.github, this.github);
  }

  @override
  Iterable<AuthMethod> get keys {
    return AuthMethod.values;
  }

  @override
  Iterable<V> get values {
    return [
      this.google,
      this.github,
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

  V get(AuthMethod key) {
    switch (key) {
      case AuthMethod.google:
        return this.google;
      case AuthMethod.github:
        return this.github;
    }
  }

  @override
  String toString() {
    final buffer = StringBuffer("{");
    buffer.write("AuthMethod.google: ${this.google}");
    buffer.write(", ");
    buffer.write("AuthMethod.github: ${this.github}");
    buffer.write("}");
    return buffer.toString();
  }
}
