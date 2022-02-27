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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.SerializableFunction;

@SuppressWarnings({"nullness"})
public class StorageApiDynamicDestinationsTableRow<T, DestinationT>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final SerializableFunction<T, TableRow> formatFunction;
  private final CreateDisposition createDisposition;

  StorageApiDynamicDestinationsTableRow(
      DynamicDestinations<T, DestinationT> inner,
      SerializableFunction<T, TableRow> formatFunction,
      CreateDisposition createDisposition) {
    super(inner);
    this.formatFunction = formatFunction;
    this.createDisposition = createDisposition;
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception {
    return new MessageConverterWithTableFieldSchema(destination, datasetService);
  }

  private class MessageConverterWithTableFieldSchema implements MessageConverter<T> {

    private final BqSchema bqFieldByName;
    private final Descriptor descriptor;

    public MessageConverterWithTableFieldSchema(
        DestinationT destination, DatasetService datasetService)
        throws IOException, InterruptedException, Descriptors.DescriptorValidationException {
      TableSchema tableSchema = getSchema(destination);
      if (tableSchema == null) {
        // If the table already exists, then try and fetch the schema from the existing
        // table.
        TableReference tableReference = getTable(destination).getTableReference();
        @Nullable Table table = datasetService.getTable(tableReference);
        if (table == null) {
          if (createDisposition == CreateDisposition.CREATE_NEVER) {
            throw new RuntimeException(
                "BigQuery table "
                    + tableReference
                    + " not found. If you wanted to "
                    + "automatically create the table, set the create disposition to CREATE_IF_NEEDED and specify a "
                    + "schema.");
          } else {
            throw new RuntimeException(
                "Schema must be set for table "
                    + tableReference
                    + " when writing TableRows using Storage API and "
                    + "using a create disposition of CREATE_IF_NEEDED.");
          }
        }
        tableSchema = table.getSchema();
      }

      bqFieldByName = BqSchema.fromTableSchema(tableSchema);
      descriptor = TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema);
    }

    @Override
    public Descriptor getSchemaDescriptor() {
      return descriptor;
    }

    @Override
    public Message toMessage(T element) {
      return TableRowToStorageApiProto.messageFromTableRow(
          bqFieldByName, descriptor, formatFunction.apply(element));
    }
  }

  static class BqSchema {
    private final TableFieldSchema tableFieldSchema;
    private final ArrayList<BqSchema> subFields;
    private final HashMap<String, BqSchema> subFieldsByName;

    private BqSchema(TableFieldSchema tableFieldSchema) {
      this.tableFieldSchema = tableFieldSchema;
      this.subFields = new ArrayList<>();
      this.subFieldsByName = new HashMap<>();
      if (tableFieldSchema.getFields() != null) {
        for (TableFieldSchema field : tableFieldSchema.getFields()) {
          BqSchema bqSchema = new BqSchema(field);
          subFields.add(bqSchema);
          subFieldsByName.put(field.getName(), bqSchema);
        }
      }
    }

    public String getName() {
      return tableFieldSchema.getName();
    }

    public String getBqType() {
      return tableFieldSchema.getType();
    }

    public BqSchema getSubFieldByName(String name) {
      return subFieldsByName.get(name);
    }

    public BqSchema getSubFieldByIndex(int i) {
      return subFields.get(i);
    }

    static BqSchema fromTableSchema(TableSchema tableSchema) {
      TableFieldSchema rootSchema =
          new TableFieldSchema()
              .setName("__root__")
              .setType("RECORD")
              .setFields(tableSchema.getFields());
      return new BqSchema(rootSchema);
    }
  }
}
