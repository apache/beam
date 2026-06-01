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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CdcResolver}. */
@RunWith(JUnit4.class)
public class CdcResolverTest {
  private static final TestResolver RESOLVER = new TestResolver();

  @Test
  public void duplicateDeleteInsertIsDropped() {
    List<String> emitted =
        resolve(
            Collections.singletonList(item("same", 7)), Collections.singletonList(item("same", 7)));

    assertThat(emitted, empty());
  }

  @Test
  public void changedDeleteInsertBecomesUpdatePair() {
    List<String> emitted =
        resolve(
            Collections.singletonList(item("before", 1)),
            Collections.singletonList(item("after", 2)));

    assertThat(emitted, contains("UPDATE_BEFORE:before", "UPDATE_AFTER:after"));
  }

  @Test
  public void duplicateUpdateAndSingletonsResolveByMultiplicity() {
    List<String> emitted =
        resolve(
            Arrays.asList(item("copy", 1), item("old", 2), item("deleted-only", 3)),
            Arrays.asList(item("copy", 1), item("new", 4)));

    assertThat(emitted, contains("UPDATE_BEFORE:old", "UPDATE_AFTER:new", "DELETE:deleted-only"));
  }

  @Test
  public void hashCollisionOnlyConsumesEqualInsertOnce() {
    List<String> emitted =
        resolve(
            Arrays.asList(item("copy", 9), item("deleted-only", 9)),
            Collections.singletonList(item("copy", 9)));

    assertThat(emitted, contains("DELETE:deleted-only"));
  }

  @Test
  public void hashMatchAloneDoesNotDeduplicate() {
    List<String> emitted =
        resolve(
            Collections.singletonList(item("before", 42)),
            Collections.singletonList(item("after", 42)));

    assertThat(emitted, contains("UPDATE_BEFORE:before", "UPDATE_AFTER:after"));
  }

  private static Item item(String nonPkValue, int hash) {
    return new Item(nonPkValue, hash);
  }

  private static List<String> resolve(List<Item> deletes, List<Item> inserts) {
    List<String> emitted = new ArrayList<>();
    RESOLVER.resolve(
        deletes, inserts, (kind, item) -> emitted.add(kind.name() + ":" + item.nonPkValue));
    return emitted;
  }

  private static class TestResolver extends CdcResolver<Item> {
    @Override
    protected int nonPkHash(Item element) {
      return element.hash;
    }

    @Override
    protected boolean nonPkEquals(Item delete, Item insert) {
      return delete.nonPkValue.equals(insert.nonPkValue);
    }
  }

  private static class Item {
    private final String nonPkValue;
    private final int hash;

    private Item(String nonPkValue, int hash) {
      this.nonPkValue = nonPkValue;
      this.hash = hash;
    }
  }
}
