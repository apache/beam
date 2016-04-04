
package cz.seznam.euphoria.core.client.functional;

/**
 * Function reducing iterable of elements into single one of the same type.
 * The applied function has to be commutative associative.
 */
@FunctionalInterface
public interface CombinableReduceFunction<T> extends ReduceFunction<T, T> {

}
