package cz.seznam.euphoria.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Util {

  public static List<String> sorted(List<String> xs) {
    return xs.stream().sorted().collect(Collectors.toList());
  }

  public static <T> List<T> sorted(Collection<T> xs, Comparator<T> c) {
    ArrayList<T> list = new ArrayList<>(xs);
    list.sort(c);
    return list;
  }

  private Util() {}
}
