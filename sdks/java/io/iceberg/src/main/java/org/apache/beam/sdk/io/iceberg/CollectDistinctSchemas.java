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
package org.apache.beam.sdk.io.iceberg;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.transforms.Combine;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * One output entry per distinct schema: its file count and the columns EVERY file carrying it
 * proved free of nulls; one file with a null in "name" forces "name" to relax, however many clean
 * files sit next to it. Partition keys in path are kept the same way: {@code dt} stays only if
 * every file sits under a {@code dt=...} folder. Entries come out most common first (ties broken by
 * the JSON text) because the commit side applies schemas in that order and the most common schema
 * should win a conflict. Schemas are compared as strings, so inputs must already be canonical.
 */
class CollectDistinctSchemas
    extends Combine.CombineFn<
        CollectDistinctSchemas.SchemaGroup,
        Map<String, CollectDistinctSchemas.Group>,
        List<CollectDistinctSchemas.SchemaGroup>> {

  /** Mutable accumulator counterpart of {@link SchemaGroup}. */
  static final class Group {
    long files;
    TreeSet<String> nullFreeColumns;
    TreeSet<String> partitionKeysInPath;

    Group(long files, TreeSet<String> nullFreeColumns, TreeSet<String> partitionKeysInPath) {
      this.files = files;
      this.nullFreeColumns = nullFreeColumns;
      this.partitionKeysInPath = partitionKeysInPath;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof Group)) {
        return false;
      }
      Group that = (Group) other;
      return files == that.files
          && nullFreeColumns.equals(that.nullFreeColumns)
          && partitionKeysInPath.equals(that.partitionKeysInPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(files, nullFreeColumns, partitionKeysInPath);
    }
  }

  /**
   * A schema, how many files carry it, and the columns all of them proved free of nulls.
   * ReadFooterSchema emits one per file ({@code files} = 1); this combiner merges them.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class SchemaGroup {
    private static @MonotonicNonNull SchemaCoder<SchemaGroup> coder;

    static SchemaGroup of(String schemaJson, long files, List<String> nullFreeColumns) {
      return of(schemaJson, files, nullFreeColumns, Collections.emptyList());
    }

    static SchemaGroup of(
        String schemaJson,
        long files,
        List<String> nullFreeColumns,
        List<String> partitionKeysInPath) {
      return new AutoValue_CollectDistinctSchemas_SchemaGroup(
          schemaJson, files, nullFreeColumns, partitionKeysInPath);
    }

    SchemaGroup withPartitionKeysInPath(List<String> partitionKeysInPath) {
      return of(getSchemaJson(), getFiles(), getNullFreeColumns(), partitionKeysInPath);
    }

    static SchemaCoder<SchemaGroup> getCoder() {
      if (coder == null) {
        try {
          coder = SchemaRegistry.createDefault().getSchemaCoder(SchemaGroup.class);
        } catch (NoSuchSchemaException e) {
          throw new RuntimeException(e);
        }
      }
      return coder;
    }

    @SchemaFieldNumber("0")
    abstract String getSchemaJson();

    @SchemaFieldNumber("1")
    abstract long getFiles();

    @SchemaFieldNumber("2")
    abstract List<String> getNullFreeColumns();

    /**
     * Folder keys every file of the group has a value for, e.g. {@code [dt]} when every file sits
     * under a {@code dt=...} folder. A table partitioned by {@code dt} then needs no {@code dt}
     * column in these files.
     */
    @SchemaFieldNumber("3")
    abstract List<String> getPartitionKeysInPath();

    @Override
    public final String toString() {
      return getFiles()
          + " file(s), null-free in "
          + getNullFreeColumns()
          + ", partition keys in path "
          + getPartitionKeysInPath()
          + ", schema "
          + getSchemaJson();
    }
  }

  @Override
  public Map<String, Group> createAccumulator() {
    return new TreeMap<>();
  }

  @Override
  public Map<String, Group> addInput(Map<String, Group> accumulator, SchemaGroup file) {
    add(
        accumulator,
        file.getSchemaJson(),
        file.getFiles(),
        file.getNullFreeColumns(),
        file.getPartitionKeysInPath());
    return accumulator;
  }

  @Override
  public Map<String, Group> mergeAccumulators(Iterable<Map<String, Group>> accumulators) {
    Map<String, Group> merged = createAccumulator();
    for (Map<String, Group> accumulator : accumulators) {
      for (Map.Entry<String, Group> entry : accumulator.entrySet()) {
        Group group = entry.getValue();
        add(merged, entry.getKey(), group.files, group.nullFreeColumns, group.partitionKeysInPath);
      }
    }
    return merged;
  }

  @Override
  public List<SchemaGroup> extractOutput(Map<String, Group> accumulator) {
    List<SchemaGroup> schemas = new ArrayList<>();
    for (Map.Entry<String, Group> entry : accumulator.entrySet()) {
      schemas.add(
          SchemaGroup.of(
              entry.getKey(),
              entry.getValue().files,
              new ArrayList<>(entry.getValue().nullFreeColumns),
              new ArrayList<>(entry.getValue().partitionKeysInPath)));
    }
    schemas.sort(
        (a, b) -> {
          int byCount = Long.compare(b.getFiles(), a.getFiles());
          if (byCount != 0) {
            return byCount;
          }
          return a.getSchemaJson().compareTo(b.getSchemaJson());
        });
    return schemas;
  }

  @Override
  public Coder<Map<String, Group>> getAccumulatorCoder(
      CoderRegistry registry, Coder<SchemaGroup> inputCoder) {
    return MapCoder.of(StringUtf8Coder.of(), GroupCoder.INSTANCE);
  }

  @Override
  public Coder<List<SchemaGroup>> getDefaultOutputCoder(
      CoderRegistry registry, Coder<SchemaGroup> inputCoder) {
    return outputCoder();
  }

  static Coder<SchemaGroup> groupCoder() {
    return SchemaGroup.getCoder();
  }

  static Coder<List<SchemaGroup>> outputCoder() {
    return ListCoder.of(SchemaGroup.getCoder());
  }

  private static final Coder<List<String>> COLUMNS_CODER = ListCoder.of(StringUtf8Coder.of());

  /** Sorted columns, so the encoding is deterministic. */
  private static class GroupCoder extends AtomicCoder<Group> {
    static final GroupCoder INSTANCE = new GroupCoder();

    private GroupCoder() {}

    @Override
    public void encode(Group value, OutputStream out) throws IOException {
      VarLongCoder.of().encode(value.files, out);
      COLUMNS_CODER.encode(new ArrayList<>(value.nullFreeColumns), out);
      COLUMNS_CODER.encode(new ArrayList<>(value.partitionKeysInPath), out);
    }

    @Override
    public Group decode(InputStream in) throws IOException {
      long files = VarLongCoder.of().decode(in);
      TreeSet<String> nullFreeColumns = new TreeSet<>(COLUMNS_CODER.decode(in));
      TreeSet<String> partitionKeysInPath = new TreeSet<>(COLUMNS_CODER.decode(in));
      return new Group(files, nullFreeColumns, partitionKeysInPath);
    }
  }

  private static void add(
      Map<String, Group> accumulator,
      String schemaJson,
      long files,
      Iterable<String> nullFreeColumns,
      Iterable<String> partitionKeysInPath) {
    Group existing = accumulator.get(schemaJson);
    if (existing == null) {
      accumulator.put(
          schemaJson, new Group(files, copy(nullFreeColumns), copy(partitionKeysInPath)));
      return;
    }
    existing.files += files;
    existing.nullFreeColumns = intersect(existing.nullFreeColumns, nullFreeColumns);
    existing.partitionKeysInPath = intersect(existing.partitionKeysInPath, partitionKeysInPath);
  }

  private static TreeSet<String> copy(Iterable<String> values) {
    TreeSet<String> copy = new TreeSet<>();
    for (String value : values) {
      copy.add(value);
    }
    return copy;
  }

  private static TreeSet<String> intersect(TreeSet<String> kept, Iterable<String> values) {
    TreeSet<String> stillKept = new TreeSet<>();
    for (String value : values) {
      if (kept.contains(value)) {
        stillKept.add(value);
      }
    }
    return stillKept;
  }
}
