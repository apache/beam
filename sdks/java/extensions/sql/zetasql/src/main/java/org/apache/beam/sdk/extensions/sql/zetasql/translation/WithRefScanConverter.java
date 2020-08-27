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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithRefScan;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;

/** Converts a call-site reference to a named WITH subquery. */
class WithRefScanConverter extends RelConverter<ResolvedWithRefScan> {

  WithRefScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedWithRefScan zetaNode) {
    // WithRefScan contains only a name of a WITH query,
    // but to actually convert it to the node we need to get the resolved node representation
    // of the query. Here we take it from the trait, where it was persisted previously
    // in WithScanConverter that actually parses the WITH query part.
    //
    // This query node returned from here will be converted by some other converter,
    // (e.g. if the WITH query root is a projection it will go through ProjectScanConverter)
    // and will reach the convert() method below as an already converted rel node.
    return Collections.singletonList(
        checkArgumentNotNull(getTrait().withEntries.get(zetaNode.getWithQueryName())).getWithSubquery());
  }

  @Override
  public RelNode convert(ResolvedWithRefScan zetaNode, List<RelNode> inputs) {
    // Here the actual WITH query body has already been converted by, e.g. a ProjectScnaConverter,
    // so to resolve the reference we just return that converter rel node.
    return inputs.get(0);
  }
}
