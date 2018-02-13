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

package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.values.RowType;

/**
 * {@code BeamTextTable} represents a text file/directory(backed by {@code TextIO}).
 */
public abstract class BeamTextTable extends BaseBeamTable implements Serializable {
  protected String filePattern;

  protected BeamTextTable(RowType rowType, String filePattern) {
    super(rowType);
    this.filePattern = filePattern;
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.BOUNDED;
  }
}
