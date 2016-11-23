package cz.seznam.euphoria.core.client.dataset;

/**
 * Default partitioner used in {@link Partitioning}. It is has its own type
 * to determine same partitioner across serializations. As the class is final
 * it cannot has different implementation. 
 */
final class DefaultPartitioner<T> extends HashPartitioner<T> {
}