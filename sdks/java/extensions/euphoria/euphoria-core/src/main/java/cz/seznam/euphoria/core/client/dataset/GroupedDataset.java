

package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.util.Pair;

/**
 * Result of a GroupBy operation. This dataset is by definition
 * partitioned by {@link KEY}.
 */
public interface GroupedDataset<KEY, T> extends Dataset<Pair<KEY, T>> {

}
