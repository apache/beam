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

package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.util.ApiSurface.containsOnlyPackages;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.util.ApiSurface;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Surface test for BeamSql api.
 */
@RunWith(JUnit4.class)
public class BeamSqlApiSurfaceTest {
  @Test
  public void testSdkApiSurface() throws Exception {

    @SuppressWarnings("unchecked")
    final Set<String> allowed =
        ImmutableSet.of(
            "org.apache.beam",
            "org.joda.time",
            "com.alibaba.fastjson",
            // exposed by fastjson
            "sun.reflect"
            );

    ApiSurface surface = ApiSurface
        .ofClass(BeamSql.class)
        .includingClass(BeamSqlCli.class)
        .includingClass(BeamSqlUdf.class)
        .includingClass(RowSqlType.class)
        .includingClass(BeamSqlSeekableTable.class)
        .pruningPrefix("java")
        .pruningPattern("org[.]apache[.]beam[.]sdk[.]extensions[.]sql[.].*Test")
        .pruningPattern("org[.]apache[.]beam[.]sdk[.]extensions[.]sql[.].*TestBase");

    assertThat(surface, containsOnlyPackages(allowed));
  }

}
