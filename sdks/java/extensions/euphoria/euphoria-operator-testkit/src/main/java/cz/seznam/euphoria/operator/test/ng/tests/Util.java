package cz.seznam.euphoria.operator.test.ng.tests;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

class Util {

  static <T> List<T> sorted(Collection<T> xs, Comparator<T> c) {
    ArrayList<T> list = new ArrayList<>(xs);
    list.sort(c);
    return list;
  }
  
  static <T extends Comparable<T>> List<T> sorted(Collection<T> xs) {
    return sorted(xs, Comparator.naturalOrder());
  }
}
