import 'package:equatable/equatable.dart';

abstract class EventContext with EquatableMixin {
  Map<String, dynamic> toJson();
}
