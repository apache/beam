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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * A convenience class for applying row updates to BigQuery using {@link
 * BigQueryIO.Write#applyRowMutations}. This class encapsulates a {@link TableRow} payload along
 * with information how to update the row. A sequence number must also be supplied to order the
 * updates. Incorrect sequence numbers will result in unexpected state in the BigQuery table.
 */
@AutoValue
public abstract class RowMutation {
  public abstract TableRow getTableRow();

  public abstract RowMutationInformation getMutationInformation();

  public static RowMutation of(TableRow tableRow, RowMutationInformation rowMutationInformation) {
    return new AutoValue_RowMutation(tableRow, rowMutationInformation);
  }

  public static class RowMutationCoder extends AtomicCoder<RowMutation> {
    private static final RowMutationCoder INSTANCE = new RowMutationCoder();

    public static RowMutationCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(RowMutation value, OutputStream outStream) throws IOException {
      TableRowJsonCoder.of().encode(value.getTableRow(), outStream);
      VarIntCoder.of()
          .encode(value.getMutationInformation().getMutationType().ordinal(), outStream);
      VarLongCoder.of().encode(value.getMutationInformation().getSequenceNumber(), outStream);
    }

    @Override
    public RowMutation decode(InputStream inStream) throws IOException {
      return RowMutation.of(
          TableRowJsonCoder.of().decode(inStream),
          RowMutationInformation.of(
              RowMutationInformation.MutationType.values()[VarIntCoder.of().decode(inStream)],
              VarLongCoder.of().decode(inStream)));
    }
  }
}
