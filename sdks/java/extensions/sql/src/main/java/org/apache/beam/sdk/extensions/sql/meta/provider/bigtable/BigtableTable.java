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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static java.util.stream.Collectors.toSet;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.COLUMNS_MAPPING;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps.newHashMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets.newHashSet;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.io.gcp.bigtable.BeamRowToBigtableMutation;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableRowToBeamRow;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableRowToBeamRowFlat;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;

public class BigtableTable extends SchemaBaseBeamTable implements Serializable {
  // Should match:
  // googleapis.com/bigtable/projects/projectId/instances/instanceId/tables/tableId"
  private static final Pattern locationPattern =
      Pattern.compile(
          "(?<host>.+)/bigtable/projects/(?<projectId>.+)/instances/(?<instanceId>.+)/tables/(?<tableId>.+)");

  private final String projectId;
  private final String instanceId;
  private final String tableId;
  private String emulatorHost = "";

  private boolean useFlatSchema = false;

  private Map<String, Set<String>> columnsMapping = newHashMap();

  BigtableTable(Table table) {
    super(table.getSchema());
    validateSchema(schema);

    String location = table.getLocation();
    if (location == null) {
      throw new IllegalStateException("LOCATION is required");
    }
    Matcher matcher = locationPattern.matcher(location);
    validateMatcher(matcher, location);

    this.projectId = getMatcherValue(matcher, "projectId");
    this.instanceId = getMatcherValue(matcher, "instanceId");
    this.tableId = getMatcherValue(matcher, "tableId");
    String host = getMatcherValue(matcher, "host"); // googleapis.com or localhost:<PORT>
    if (!"googleapis.com".equals(host)) {
      this.emulatorHost = host;
    }

    JSONObject properties = table.getProperties();
    if (properties.containsKey(COLUMNS_MAPPING)) {
      columnsMapping = parseColumnsMapping(properties.getString(COLUMNS_MAPPING));
      validateColumnsMapping(columnsMapping, schema);
      useFlatSchema = true;
    }
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return readTransform()
        .expand(begin)
        .apply("BigtableRowToBeamRow", bigtableRowToRow())
        .setRowSchema(schema);
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    BigtableIO.Read readTransform = readTransform();
    if (filters instanceof BigtableFilter) {
      BigtableFilter bigtableFilter = (BigtableFilter) filters;
      readTransform = readTransform.withRowFilter(bigtableFilter.getFilters());
    }
    return readTransform.expand(begin).apply(bigtableRowToRow());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    if (!useFlatSchema) {
      throw new UnsupportedOperationException(
          "Write to Cloud Bigtable is supported for flat schema only.");
    }
    BigtableIO.Write write =
        BigtableIO.write().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId);
    if (!emulatorHost.isEmpty()) {
      write = write.withEmulator(emulatorHost);
    }
    return input.apply(new BeamRowToBigtableMutation(columnsMapping)).apply(write);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    return new BigtableFilter(filter, schema);
  }

  private static Map<String, Set<String>> parseColumnsMapping(String commaSeparatedMapping) {
    Map<String, Set<String>> columnsMapping = new HashMap<>();
    Splitter.on(",")
        .splitToList(commaSeparatedMapping)
        .forEach(
            colonSeparatedValues -> {
              List<String> pair = Splitter.on(":").splitToList(colonSeparatedValues);
              columnsMapping.putIfAbsent(pair.get(0), newHashSet());
              columnsMapping.get(pair.get(0)).add(pair.get(1));
            });
    return columnsMapping;
  }

  private static String getMatcherValue(Matcher matcher, String field) {
    String value = matcher.group(field);
    return value == null ? "" : value;
  }

  private static void validateSchema(Schema schema) {
    if (!schema.hasField(KEY)) {
      throw new IllegalStateException(String.format("Schema has to contain '%s' field", KEY));
    } else {
      Schema.Field keyField = schema.getField(KEY);
      if (keyField != null && !(Schema.TypeName.STRING == keyField.getType().getTypeName())) {
        throw new IllegalArgumentException(
            "key field type should be STRING but was " + keyField.getType().getTypeName());
      }
    }
  }

  private static void validateMatcher(Matcher matcher, String location) {
    if (!matcher.matches()) {
      throw new InvalidTableException(
          "Bigtable location must be in the following format:"
              + " 'googleapis.com/bigtable/projects/projectId/instances/instanceId/tables/tableId'"
              + " but was: "
              + location);
    }
  }

  private static void validateColumnsMapping(
      Map<String, Set<String>> columnsMapping, Schema schema) {
    validateColumnsMappingCount(columnsMapping, schema);
    validateColumnsMappingFields(columnsMapping, schema);
  }

  private static void validateColumnsMappingCount(
      Map<String, Set<String>> columnsMapping, Schema schema) {
    int mappingCount = columnsMapping.values().stream().mapToInt(Set::size).sum();
    // Don't count the key field
    int qualifiersCount = schema.getFieldCount() - 1;
    if (qualifiersCount != mappingCount) {
      throw new IllegalStateException(
          String.format(
              "Schema fields count: '%s' does not fit columnsMapping count: '%s'",
              qualifiersCount, mappingCount));
    }
  }

  private static void validateColumnsMappingFields(
      Map<String, Set<String>> columnsMapping, Schema schema) {
    Set<String> allMappingQualifiers =
        columnsMapping.values().stream().flatMap(Collection::stream).collect(toSet());

    Set<String> schemaFieldNames =
        schema.getFieldNames().stream().filter(field -> !KEY.equals(field)).collect(toSet());

    if (!schemaFieldNames.equals(allMappingQualifiers)) {
      throw new IllegalStateException(
          String.format(
              "columnsMapping '%s' does not fit to schema field names '%s'",
              allMappingQualifiers, schemaFieldNames));
    }
  }

  private BigtableIO.Read readTransform() {
    BigtableIO.Read readTransform =
        BigtableIO.read().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId);
    if (!emulatorHost.isEmpty()) {
      readTransform = readTransform.withEmulator(emulatorHost);
    }
    return readTransform;
  }

  private PTransform<PCollection<com.google.bigtable.v2.Row>, PCollection<Row>> bigtableRowToRow() {
    return useFlatSchema
        ? new BigtableRowToBeamRowFlat(schema, columnsMapping)
        : new BigtableRowToBeamRow(schema);
  }
}
