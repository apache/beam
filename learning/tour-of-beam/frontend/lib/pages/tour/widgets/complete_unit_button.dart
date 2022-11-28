import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../../cache/user_progress.dart';
import '../state.dart';

class CompleteUnitButton extends StatelessWidget {
  final TourNotifier tourNotifier;
  const CompleteUnitButton(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);
    final userProgressCache = GetIt.instance.get<UserProgressCache>();

    return AnimatedBuilder(
      animation: userProgressCache,
      builder: (context, child) {
        return Flexible(
          child: OutlinedButton(
            style: OutlinedButton.styleFrom(
              foregroundColor: themeData.primaryColor,
              side: BorderSide(
                color: tourNotifier.canCompleteCurrentUnit()
                    ? themeData.primaryColor
                    : themeData.disabledColor,
              ),
              shape: const RoundedRectangleBorder(
                borderRadius: BorderRadius.all(
                  Radius.circular(BeamSizes.size4),
                ),
              ),
            ),
            onPressed: tourNotifier.canCompleteCurrentUnit()
                ? tourNotifier.currentUnitController?.completeUnit
                : null,
            child: const Text(
              'pages.tour.completeUnit',
              overflow: TextOverflow.ellipsis,
            ).tr(),
          ),
        );
      },
    );
  }
}
