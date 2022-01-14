import 'package:flutter/material.dart';
import 'package:playground/constants/colors.dart';

class LoadingIndicator extends StatelessWidget {
  final double size;

  const LoadingIndicator({Key? key, required this.size}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Center(
      child: SizedBox(
        height: size,
        width: size,
        child: const CircularProgressIndicator(
          color: kLightPrimary,
        ),
      ),
    );
  }
}
