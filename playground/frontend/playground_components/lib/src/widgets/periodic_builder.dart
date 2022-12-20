import 'dart:async';

import 'package:flutter/material.dart';

class PeriodicBuilderWidget extends StatefulWidget {
  final Duration interval;
  final ValueGetter<Widget> builder;

  const PeriodicBuilderWidget({
    super.key,
    required this.interval,
    required this.builder,
  });

  @override
  State<PeriodicBuilderWidget> createState() => _PeriodicBuilderWidgetState();
}

class _PeriodicBuilderWidgetState extends State<PeriodicBuilderWidget> {
  late Timer _timer;

  @override
  void initState() {
    super.initState();
    _timer = Timer.periodic(
      widget.interval,
      (_) {
        setState(() {});
      },
    );
  }

  @override
  void dispose() {
    super.dispose();
    _timer.cancel();
  }

  @override
  Widget build(BuildContext context) {
    return widget.builder();
  }
}
