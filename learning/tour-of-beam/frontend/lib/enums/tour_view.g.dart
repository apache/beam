// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'tour_view.dart';

// **************************************************************************
// UnmodifiableEnumMapGenerator
// **************************************************************************

class UnmodifiableTourViewMap<V> extends UnmodifiableEnumMap<TourView, V> {
  final V content;
  final V playground;

  const UnmodifiableTourViewMap({
    required this.content,
    required this.playground,
  });

  @override
  Map<RK, RV> cast<RK, RV>() {
    return Map.castFrom<TourView, V, RK, RV>(this);
  }

  @override
  bool containsValue(Object? value) {
    if (this.content == value) return true;
    if (this.playground == value) return true;
    return false;
  }

  @override
  bool containsKey(Object? key) {
    return key.runtimeType == TourView;
  }

  @override
  V? operator [](Object? key) {
    switch (key) {
      case TourView.content:
        return this.content;
      case TourView.playground:
        return this.playground;
    }

    return null;
  }

  @override
  void operator []=(TourView key, V value) {
    throw Exception("Cannot modify this map.");
  }

  @override
  Iterable<MapEntry<TourView, V>> get entries {
    return [
      MapEntry<TourView, V>(TourView.content, this.content),
      MapEntry<TourView, V>(TourView.playground, this.playground),
    ];
  }

  @override
  Map<K2, V2> map<K2, V2>(MapEntry<K2, V2> transform(TourView key, V value)) {
    final content = transform(TourView.content, this.content);
    final playground = transform(TourView.playground, this.playground);
    return {
      content.key: content.value,
      playground.key: playground.value,
    };
  }

  @override
  void addEntries(Iterable<MapEntry<TourView, V>> newEntries) {
    throw Exception("Cannot modify this map.");
  }

  @override
  V update(TourView key, V update(V value), {V Function()? ifAbsent}) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void updateAll(V update(TourView key, V value)) {
    throw Exception("Cannot modify this map.");
  }

  @override
  void removeWhere(bool test(TourView key, V value)) {
    throw Exception("Objects in this map cannot be removed.");
  }

  @override
  V putIfAbsent(TourView key, V ifAbsent()) {
    return this.get(key);
  }

  @override
  void addAll(Map<TourView, V> other) {
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
  void forEach(void action(TourView key, V value)) {
    action(TourView.content, this.content);
    action(TourView.playground, this.playground);
  }

  @override
  Iterable<TourView> get keys {
    return TourView.values;
  }

  @override
  Iterable<V> get values {
    return [
      this.content,
      this.playground,
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

  V get(TourView key) {
    switch (key) {
      case TourView.content:
        return this.content;
      case TourView.playground:
        return this.playground;
    }
  }

  @override
  String toString() {
    final buffer = StringBuffer("{");
    buffer.write("TourView.content: ${this.content}");
    buffer.write(", ");
    buffer.write("TourView.playground: ${this.playground}");
    buffer.write("}");
    return buffer.toString();
  }
}
