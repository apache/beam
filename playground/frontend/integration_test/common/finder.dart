import 'package:flutter/painting.dart';
import 'package:flutter_test/flutter_test.dart';

extension FinderExtension on Finder {
  Finder getChildrenByType(Type childType) {
    final finders = evaluate();
    final childElements = finders
        .map((f) => collectAllElementsFrom(f, skipOffstage: true))
        .expand((e) => e)
        .where((e) => e.widget.runtimeType == childType);

    return find.byElementPredicate(
      (element) => childElements.contains(element),
    );
  }

  Finder alignedIndexAt(int index, Axis axis, WidgetTester wt) {
    final finders = evaluate();

    if (index > finders.length - 1) {
      throw IndexError(index, finders);
    }

    final offsets = <_IndexAndOffset>[];

    for (int i = 0; i < finders.length; i++) {
      offsets.add(_IndexAndOffset(i, wt.getCenter(at(i))));
    }

    offsets.sort(
      (a, b) => axis == Axis.vertical
          ? _compareDoubles(a.offset.dy, b.offset.dy)
          : _compareDoubles(a.offset.dx, b.offset.dx),
    );

    final result = find.byElementPredicate((element) {
      return axis == Axis.vertical
          ? finders.contains(element) &&
              wt.getCenter(at(finders.toList().indexOf(element))).dy ==
                  offsets[index].offset.dy
          : finders.contains(element) &&
              wt.getCenter(at(finders.toList().indexOf(element))).dx ==
                  offsets[index].offset.dx;
    });

    print(result.evaluate().length);
    return result;
  }

  int _compareDoubles(double a, double b) {
    if (a == b) {
      return 0;
    } else if (a > b) {
      return 1;
    } else {
      return -1;
    }
  }

  Finder horizontallyAt(int index, WidgetTester wt) =>
      alignedIndexAt(index, Axis.horizontal, wt);

  Finder verticallyAt(int index, WidgetTester wt) =>
      alignedIndexAt(index, Axis.vertical, wt);
}

class _IndexAndOffset {
  final int index;
  final Offset offset;

  _IndexAndOffset(this.index, this.offset);
}
