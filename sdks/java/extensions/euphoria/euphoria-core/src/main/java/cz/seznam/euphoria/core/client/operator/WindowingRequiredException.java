package cz.seznam.euphoria.core.client.operator;

/**
 * Thrown by executors at flow submission time when an invalid flow set up is detected,
 * requiring the user to explicitly provide a windowing strategy to a certain operator.
 */
public class WindowingRequiredException extends IllegalStateException {
  public WindowingRequiredException(String message) {
    super(message);
  }
}
