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
package org.apache.beam.it.gcp.bigtable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.threeten.bp.Duration;

public class BigtableTableSpec {
  private Set<String> columnFamilies;
  private Duration maxAge;
  private boolean cdcEnabled;

  public BigtableTableSpec() {
    this.maxAge = Duration.ofHours(1);
    this.columnFamilies = new HashSet<>();
    this.cdcEnabled = false;
  }

  public Set<String> getColumnFamilies() {
    return Collections.unmodifiableSet(columnFamilies);
  }

  public void setColumnFamilies(Set<String> columnFamilies) {
    this.columnFamilies = new HashSet<>(columnFamilies);
  }

  public void setColumnFamilies(Iterable<String> columnFamilies) {
    this.columnFamilies = new HashSet<>();
    columnFamilies.forEach(this.columnFamilies::add);
  }

  public Duration getMaxAge() {
    return maxAge;
  }

  public void setMaxAge(Duration maxAge) {
    this.maxAge = maxAge;
  }

  public boolean getCdcEnabled() {
    return cdcEnabled;
  }

  public void setCdcEnabled(boolean cdcEnabled) {
    this.cdcEnabled = cdcEnabled;
  }
}
