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

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithScan;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;

/** Converts a named WITH. */
class WithScanConverter extends RelConverter<ResolvedWithScan> {

  WithScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedWithScan zetaNode) {
    // We must persist the named WITH queries nodes,
    // so that when they are referenced by name (e.g. in FROM/JOIN), we can
    // resolve them. We need this because the nodes that represent the references (WithRefScan)
    // only contain the names of the queries, so we need to keep this map for resolution of the
    // names.
    zetaNode
        .getWithEntryList()
        .forEach(withEntry -> getTrait().withEntries.put(withEntry.getWithQueryName(), withEntry));

    // Returning the body of the query, it is something like ProjectScan that will be converted
    // by ProjectScanConverter before it reaches the convert() method below.
    return Collections.singletonList(zetaNode.getQuery());
  }

  @Override
  public RelNode convert(ResolvedWithScan zetaNode, List<RelNode> inputs) {
    // The body of the WITH query is already converted at this point so we just
    // return it, nothing else is needed.
    return inputs.get(0);
  }
}
