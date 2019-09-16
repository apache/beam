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

import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByScan;
import java.util.List;
import org.apache.calcite.rel.RelNode;

/**
 * Always throws exception, represents the case when order by is used without limit.
 *
 * <p>Order by limit is a special case that is handled in {@link LimitOffsetScanToLimitConverter}.
 */
class OrderByScanUnsupportedConverter extends RelConverter<ResolvedOrderByScan> {

  OrderByScanUnsupportedConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public RelNode convert(ResolvedOrderByScan zetaNode, List<RelNode> inputs) {
    throw new UnsupportedOperationException("ORDER BY without a LIMIT is not supported.");
  }
}
