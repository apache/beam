

package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.operator.Pair;

/**
 * Result of a GroupBy operation. This is just a labeling interface.
 */
public interface GroupedDataset<KEY, T> extends Dataset<Pair<KEY, T>> {

}
