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
package org.apache.beam.sdk.tpcds;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;

import java.io.Serializable;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.csvLines2BeamRows;

/**
 * A readConverter class for TextTable that can read csv file and transform it to PCollection<Row>
 */
public class CsvToRow extends PTransform<PCollection<String>, PCollection<Row>>
        implements Serializable {
    private Schema schema;
    private CSVFormat csvFormat;

    public CSVFormat getCsvFormat() {
        return csvFormat;
    }

    public CsvToRow(Schema schema, CSVFormat csvFormat) {
        this.schema = schema;
        this.csvFormat = csvFormat;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> input) {
        return input
                .apply(
                        "csvToRow",
                        FlatMapElements.into(TypeDescriptors.rows())
                                .via(s -> csvLines2BeamRows(csvFormat, s, schema)))
                .setRowSchema(schema);
    }
}
