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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryWriteConfiguration.TimePartitioningConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.util.RowStringInterpolator;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
public class PortableBigQueryDestinations extends DynamicDestinations<Row, String> {
  public static final String DESTINATION = "destination";
  public static final String RECORD = "record";
  private @MonotonicNonNull RowStringInterpolator interpolator = null;
  private final @Nullable List<String> primaryKey;
  private final RowFilter rowFilter;
  private final @Nullable List<String> clusteringFields;
  private final @Nullable TimePartitioningConfig timePartitioningConfig;

  public PortableBigQueryDestinations(Schema rowSchema, BigQueryWriteConfiguration configuration) {
    this.clusteringFields = configuration.getClusteringFields();
    this.timePartitioningConfig = configuration.getTimePartitioningConfig();

    // Validate partition field exists if time partitioning field is set
    if (this.timePartitioningConfig != null && this.timePartitioningConfig.getField() != null) {
      String partitionField = this.timePartitioningConfig.getField();

      // Check if the partition field exists in the schema
      boolean fieldExists =
          rowSchema.getFields().stream().anyMatch(field -> field.getName().equals(partitionField));
      if (!fieldExists) {
        throw new IllegalArgumentException(
            String.format(
                "The partition field '%s' does not exist in the input schema.", partitionField));
      }
    }
    // DYNAMIC_DESTINATIONS magic string is the old way of doing it for cross-language.
    // In that case, we do no interpolation
    if (!configuration.getTable().equals(DYNAMIC_DESTINATIONS)) {
      this.interpolator = new RowStringInterpolator(configuration.getTable(), rowSchema);
    }
    this.primaryKey = configuration.getPrimaryKey();
    RowFilter rf = new RowFilter(rowSchema);
    if (configuration.getDrop() != null) {
      rf = rf.drop(checkStateNotNull(configuration.getDrop()));
    }
    if (configuration.getKeep() != null) {
      rf = rf.keep(checkStateNotNull(configuration.getKeep()));
    }
    if (configuration.getOnly() != null) {
      rf = rf.only(checkStateNotNull(configuration.getOnly()));
    }
    this.rowFilter = rf;
  }

  @Override
  public String getDestination(@Nullable ValueInSingleWindow<Row> element) {
    if (interpolator != null) {
      return interpolator.interpolate(checkArgumentNotNull(element));
    }
    return checkStateNotNull(checkStateNotNull(element).getValue().getString(DESTINATION));
  }

  @Override
  public TableDestination getTable(String destination) {

    TimePartitioning timePartitioning = null;

    if (timePartitioningConfig != null) {
      String type = timePartitioningConfig.getType();
      String field = timePartitioningConfig.getField();
      Long expirationMs = timePartitioningConfig.getExpirationMs();
      Boolean requirePartitionFilter = timePartitioningConfig.getRequirePartitionFilter();

      Set<String> allowedTypes = new HashSet<>(Arrays.asList("DAY", "HOUR", "MONTH", "YEAR"));
      if (!allowedTypes.contains(type)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid TimePartitioning 'type': '%s'. Allowed values are: %s",
                type, allowedTypes));
      }

      timePartitioning =
          new TimePartitioning().setType(type); // type is required, as checked earlier

      if (field != null) {
        timePartitioning.setField(field);
      }

      if (expirationMs != null) {
        timePartitioning.setExpirationMs(expirationMs);
      }

      if (requirePartitionFilter != null) {
        timePartitioning.setRequirePartitionFilter(requirePartitionFilter);
      }
    }

    if (clusteringFields != null && !clusteringFields.isEmpty()) {
      Clustering clustering = new Clustering().setFields(clusteringFields);
      return new TableDestination(destination, null, timePartitioning, clustering);
    } else if (timePartitioning != null) {
      return new TableDestination(destination, null, timePartitioning);
    }
    return new TableDestination(destination, null);
  }

  @Override
  public @Nullable TableSchema getSchema(String destination) {
    return BigQueryUtils.toTableSchema(rowFilter.outputSchema());
  }

  @Override
  public @Nullable TableConstraints getTableConstraints(String destination) {
    if (primaryKey != null) {
      return new TableConstraints()
          .setPrimaryKey(new TableConstraints.PrimaryKey().setColumns(primaryKey));
    }
    return null;
  }

  public SerializableFunction<Row, TableRow> getFilterFormatFunction(boolean fetchNestedRecord) {
    return row -> {
      if (fetchNestedRecord) {
        row = checkStateNotNull(row.getRow(RECORD));
      }
      Row filtered = rowFilter.filter(row);
      return BigQueryUtils.toTableRow(filtered);
    };
  }

  public SerializableFunction<AvroWriteRequest<Row>, GenericRecord> getAvroFilterFormatFunction(
      boolean fetchNestedRecord) {
    return request -> {
      Row row = request.getElement();
      if (fetchNestedRecord) {
        row = checkStateNotNull(row.getRow(RECORD));
      }
      Row filtered = rowFilter.filter(row);
      return AvroUtils.toGenericRecord(filtered);
    };
  }
}
