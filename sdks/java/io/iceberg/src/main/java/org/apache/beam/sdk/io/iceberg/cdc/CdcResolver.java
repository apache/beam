/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg.cdc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.iceberg.data.Record;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Helper class to reconcile CDC rows. Used by {@link ResolveChanges} (with Beam {@link Row}s) and
 * {@link LocalResolveDoFn} (with Iceberg {@link Record}s).
 *
 * <p>For rows that share a given Primary Key, we determine the output ValueKind as follows:
 *
 * <ul>
 *   <li>(delete, insert) pairs become {@code UPDATE_BEFORE} + {@code UPDATE_AFTER}
 *   <li>singletons remain {@code DELETE} or {@code INSERT}
 *   <li>matching delete+insert with identical non-PK fields are considered Copy-on-Write side
 *       effects and are dropped
 * </ul>
 *
 * <p>General implementation:
 *
 * <ol>
 *   <li>Hash-index inserts by their non-PK field hash, for efficient Copy-on-Write detection.
 *   <li>Skip matching (delete, insert) pairs with identical non-PK columns. A CoW operation deletes
 *       and rewrites the whole file (minus some records that are actually marked for deletion).
 *       Unchanged records are no-ops and should not be mistaken for updates.
 *   <li>Walk the remaining deletes and inserts, emitting matched pairs as {@link
 *       ValueKind#UPDATE_BEFORE} / {@link ValueKind#UPDATE_AFTER}.
 *   <li>Emit any unmatched extras as {@link ValueKind#DELETE} / {@link ValueKind#INSERT}.
 * </ol>
 *
 * <h3>Duplicate identifier values</h3>
 *
 * <p>Iceberg does not enforce PK uniqueness, so a single PK group may contain more than one delete
 * and/or insert (although it would be unusual). In the normal case, identifier values are unique
 * and a snapshot contributes at most one delete and one insert per PK.
 *
 * <p>If duplicates are encountered, we do not fail. Instead, we pair off the deletes and inserts,
 * and any leftovers are emitted as plain {@code DELETE} / {@code INSERT}. The pairing is
 * necessarily arbitrary as Iceberg only keeps track of commit-level sequencing. We do not have
 * further insight within a commit to determine record ordering. To produce deterministic outputs in
 * the duplicate case, both sides are ordered by {@link #nonPkHash} before pairing.
 */
abstract class CdcResolver<T> {
  /** Hashes the non-PK fields of an element. Used as the index for O(n+m) CoW deduplication. */
  protected abstract int nonPkHash(T element);

  /**
   * Returns true if two records (already known to share a PK) share identical non-PK fields. Called
   * only when the two elements collide in the {@link #nonPkHash} index, so the implementation can
   * stay simple (linear scan of non-PK fields).
   */
  protected abstract boolean nonPkEquals(T delete, T insert);

  /**
   * Resolves a Primary Key group of deletes and inserts. Caller provides {@code emit} which decides
   * how to materialize each output.
   *
   * <p>The dominant case (unique identifier values) is exactly one delete and one insert, decided
   * directly by a single {@link #nonPkEquals} with no hashing. In the rare case of duplicate PKs
   * within a snapshot, one side may hold more than one record. When this happens, we re-order the
   * lists by {@link #nonPkHash} so the result is deterministic.
   */
  final void resolve(List<T> deletes, List<T> inserts, BiConsumer<ValueKind, T> emit) {
    if (deletes.size() == 1 && inserts.size() == 1) {
      // No-op if non-PK fields are equal, otherwise we emit an update pair
      T delete = deletes.get(0);
      T insert = inserts.get(0);
      if (!nonPkEquals(delete, insert)) {
        emit.accept(ValueKind.UPDATE_BEFORE, delete);
        emit.accept(ValueKind.UPDATE_AFTER, insert);
      }
      return;
    }

    if (deletes.size() > 1 || inserts.size() > 1) {
      resolveOrdered(sortedByNonPkHash(deletes), sortedByNonPkHash(inserts), emit);
    } else {
      resolveOrdered(deletes, inserts, emit);
    }
  }

  private List<T> sortedByNonPkHash(List<T> records) {
    List<T> sorted = new ArrayList<>(records);
    sorted.sort(Comparator.comparingInt(this::nonPkHash));
    return sorted;
  }

  private void resolveOrdered(List<T> deletes, List<T> inserts, BiConsumer<ValueKind, T> emit) {
    boolean hasDeletes = !deletes.isEmpty();
    boolean hasInserts = !inserts.isEmpty();

    if (hasInserts && hasDeletes) {
      // First, check if any (delete, insert) pairs are duplicates that should not be
      // included in the output
      boolean[] dupDeletes = new boolean[deletes.size()];
      boolean[] dupInserts = new boolean[inserts.size()];

      // Map hash to insert-indices
      Map<Integer, List<Integer>> insertHashToIdx = new HashMap<>();
      for (int insertIdx = 0; insertIdx < inserts.size(); insertIdx++) {
        int insertHash = nonPkHash(inserts.get(insertIdx));
        insertHashToIdx.computeIfAbsent(insertHash, k -> new ArrayList<>()).add(insertIdx);
      }
      for (int deleteIdx = 0; deleteIdx < deletes.size(); deleteIdx++) {
        int deleteHash = nonPkHash(deletes.get(deleteIdx));
        @Nullable List<Integer> candidates = insertHashToIdx.get(deleteHash);
        if (candidates != null) {
          // check if candidates are just duplicates (e.g. from CoW)
          for (int idx = 0; idx < candidates.size(); idx++) {
            int insertIdx = candidates.get(idx);
            if (!dupInserts[insertIdx]
                && nonPkEquals(deletes.get(deleteIdx), inserts.get(insertIdx))) {
              // this (delete, insert) pair is a duplicate --> should be skipped
              dupDeletes[deleteIdx] = true;
              dupInserts[insertIdx] = true;
              candidates.remove(idx);
              break;
            }
          }
        }
      }

      // Emit matched pairs as UPDATE_BEFORE / UPDATE_AFTER.
      int d = 0;
      int i = 0;
      while (d < deletes.size() && i < inserts.size()) {
        // skip duplicates
        while (d < deletes.size() && dupDeletes[d]) {
          d++;
        }
        while (i < inserts.size() && dupInserts[i]) {
          i++;
        }

        if (d < deletes.size() && i < inserts.size()) {
          emit.accept(ValueKind.UPDATE_BEFORE, deletes.get(d));
          emit.accept(ValueKind.UPDATE_AFTER, inserts.get(i));
          d++;
          i++;
        }
      }

      // emit unmatched extras as DELETE / INSERT.
      while (d < deletes.size()) {
        if (!dupDeletes[d]) {
          emit.accept(ValueKind.DELETE, deletes.get(d));
        }
        d++;
      }
      while (i < inserts.size()) {
        if (!dupInserts[i]) {
          emit.accept(ValueKind.INSERT, inserts.get(i));
        }
        i++;
      }
    } else if (hasInserts) {
      for (T r : inserts) {
        emit.accept(ValueKind.INSERT, r);
      }
    } else if (hasDeletes) {
      for (T r : deletes) {
        emit.accept(ValueKind.DELETE, r);
      }
    }
  }
}
