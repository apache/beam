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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

public class ParquetTable extends BaseBeamTable implements Serializable {
    private final String filePattern;

    public ParquetTable(
        Schema schema,
        String filePattern){
        super(schema);
        this.filePattern = filePattern;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
        return null;
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
        return null;
    }

    @Override
    public PCollection.IsBounded isBounded() {
        return PCollection.IsBounded.BOUNDED;
    }
}
