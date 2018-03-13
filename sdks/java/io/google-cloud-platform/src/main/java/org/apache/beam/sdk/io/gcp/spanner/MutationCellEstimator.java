package org.apache.beam.sdk.io.gcp.spanner;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.function.ToLongFunction;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema.Column;

/**
 * Estimate the number of cells modified in a {@link MutationGroup}.
 */
final class MutationCellEstimator implements ToLongFunction<MutationGroup> {
  private final LoadingCache<String, ImmutableMap<String, Long>> tables;
  private final long maxNumMutations;

  MutationCellEstimator(SpannerSchema spannerSchema, long maxNumMutations) {
    tables = CacheBuilder.newBuilder()
        .initialCapacity(spannerSchema.getTables().size())
        .concurrencyLevel(1)
        .build(CacheLoader.<String, ImmutableMap<String, Long>>from(table ->
            spannerSchema
                .getColumns(table)
                .stream()
                .collect(toImmutableMap(
                    Column::getName,
                    Column::getCellsMutated))));

    this.maxNumMutations = maxNumMutations;
  }

  @Override
  public long applyAsLong(MutationGroup mutationGroup) {
    long mutatedCells = 0L;
    for (Mutation mutation : mutationGroup) {
      final ImmutableMap<String, Long> columnCells = tables.getUnchecked(mutation.getTable());

      if (mutation.getOperation() != Op.DELETE) {
        // sum the cells of the columns included in the mutation
        for (String column : mutation.getColumns()) {
          mutatedCells += columnCells.getOrDefault(column, 1L);
        }
      } else {
        // deletes are a little less obvious
        final KeySet keySet = mutation.getKeySet();

        // for single keys simply sum up all the columns in the schema
        final long rows = Iterables.size(keySet.getKeys());
        for (long cells : columnCells.values()) {
          mutatedCells += rows * cells;
        }

        // ranges should already be broken up into individual batches
        // but just in case, make a worst-case estimate about the size
        // of the key range so they will get their own transaction
        final long ranges = Iterables.size(keySet.getRanges());
        mutatedCells += maxNumMutations * ranges;
      }
    }

    return mutatedCells;
  }
}
