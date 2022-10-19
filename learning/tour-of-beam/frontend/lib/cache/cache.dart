import 'package:flutter/material.dart';

import '../repositories/client/client.dart';

abstract class Cache extends ChangeNotifier {
  final TobClient client;

  Cache({required this.client});
}
