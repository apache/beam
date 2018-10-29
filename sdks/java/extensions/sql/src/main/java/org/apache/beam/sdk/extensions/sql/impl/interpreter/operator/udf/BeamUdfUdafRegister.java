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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.udf;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.udf.BeamBoundedWindowUdf.BoundedWindowFunc;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.udf.BeamPaneInfoUdf.PaneFunc;
import org.apache.beam.sdk.extensions.sql.meta.provider.UdfUdafProvider;

/** Register UDF/UDAF provided by Apache Beam. */
@AutoService(UdfUdafProvider.class)
public class BeamUdfUdafRegister implements UdfUdafProvider {

  @Override
  public Map<String, Class<? extends BeamSqlUdf>> getBeamSqlUdfs() {
    Map<String, Class<? extends BeamSqlUdf>> udfs =
        new HashMap<String, Class<? extends BeamSqlUdf>>();
    udfs.put(PaneFunc.FIRST_PANE.name(), BeamPaneInfoUdf.BeamUdfIsFirstPane.class);
    udfs.put(PaneFunc.LAST_PANE.name(), BeamPaneInfoUdf.BeamUdfIsLastPane.class);
    udfs.put(PaneFunc.PANE_INDEX.name(), BeamPaneInfoUdf.BeamUdfPaneIndex.class);
    udfs.put(PaneFunc.PANE_TIMING.name(), BeamPaneInfoUdf.BeamUdfPaneTiming.class);

    udfs.put(BoundedWindowFunc.WINDOW_TYPE.name(), BeamBoundedWindowUdf.BeamUdfWindowType.class);
    udfs.put(BoundedWindowFunc.WINDOW_START.name(), BeamBoundedWindowUdf.BeamUdfWindowStart.class);
    udfs.put(BoundedWindowFunc.WINDOW_END.name(), BeamBoundedWindowUdf.BeamUdfWindowEnd.class);

    return ImmutableMap.copyOf(udfs);
  }
}
