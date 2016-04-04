
package cz.seznam.euphoria.core.client.dataset;

/**
 * Partitioner by hash of input.
 */
public class HashPartitioner<T> implements Partitioner<T> {

  @Override
  public int getPartition(T element)
  {
    return element.hashCode();
  }

}
