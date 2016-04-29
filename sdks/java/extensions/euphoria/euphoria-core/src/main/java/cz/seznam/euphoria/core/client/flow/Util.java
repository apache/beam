package cz.seznam.euphoria.core.client.flow;

class Util {

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
