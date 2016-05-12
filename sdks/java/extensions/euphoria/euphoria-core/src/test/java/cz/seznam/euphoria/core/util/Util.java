package cz.seznam.euphoria.core.util;

import java.util.List;
import java.util.stream.Collectors;

public class Util {

  public static List<String> sorted(List<String> xs) {
    return xs.stream().sorted().collect(Collectors.toList());
  }

  private Util() {}
}
