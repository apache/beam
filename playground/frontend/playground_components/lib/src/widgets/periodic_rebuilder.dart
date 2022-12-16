import 'dart:async';

import 'package:flutter/material.dart';

class PeriodicRebuilderWidget extends StatefulWidget {
  final int rebuildFrequencyMls;
  final Widget Function() builder;

  const PeriodicRebuilderWidget({
    super.key,
    required this.rebuildFrequencyMls,
    required this.builder,
  });

  @override
  State<PeriodicRebuilderWidget> createState() =>
      _PeriodicRebuilderWidgetState();
}

class _PeriodicRebuilderWidgetState extends State<PeriodicRebuilderWidget> {
  late Timer _timer;

  @override
  void initState() {
    super.initState();
    _timer = Timer.periodic(
      Duration(milliseconds: widget.rebuildFrequencyMls),
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
