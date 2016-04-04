
package cz.seznam.euphoria.core.client.functional;

/**
 * Function of two arguments of the same type.
 */
@FunctionalInterface
public interface SameArgsBinaryFunction<IN, OUT>
    extends BinaryFunction<IN, IN, OUT> {
  
}
