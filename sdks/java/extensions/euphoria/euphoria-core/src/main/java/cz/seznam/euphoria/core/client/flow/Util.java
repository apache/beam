package cz.seznam.euphoria.core.client.flow;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.Operator;

class Util {

  static <IN, OUT, TYPE extends Dataset<OUT>, OP extends Operator<IN, OUT, TYPE>>
  OP registerWithFlow(OP op, String logicalName)
  {
    return op.getFlow().add(op, logicalName);
  }

  /**
   * Trim string (instead of empty string return null)
   *
   * @param s input string
   * @return non-empty trimmed string or null
   */
  static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    if (s.isEmpty()) {
      return null;
    }
    return s;
  }

  private Util() {}
}
