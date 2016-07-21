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

package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.theInstance;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Tests for {@link CloningThreadLocalTest}.
 */
@RunWith(JUnit4.class)
public class CloningThreadLocalTest {
  @Test
  public void returnsCopiesOfOriginal() throws Exception {
    Record original = new Record();
    ThreadLocal<Record> loaded = CloningThreadLocal.of(original);
    assertThat(loaded.get(), not(nullValue()));
    assertThat(loaded.get(), equalTo(original));
    assertThat(loaded.get(), not(theInstance(original)));
  }

  @Test
  public void returnsDifferentCopiesInDifferentThreads() throws Exception {
    Record original = new Record();
    final ThreadLocal<Record> loaded = CloningThreadLocal.of(original);
    assertThat(loaded.get(), not(nullValue()));
    assertThat(loaded.get(), equalTo(original));
    assertThat(loaded.get(), not(theInstance(original)));

    Callable<Record> otherThread =
        new Callable<Record>() {
          @Override
          public Record call() throws Exception {
            return loaded.get();
          }
        };
    Record sameThread = loaded.get();
    Record firstOtherThread = Executors.newSingleThreadExecutor().submit(otherThread).get();
    Record secondOtherThread = Executors.newSingleThreadExecutor().submit(otherThread).get();

    assertThat(sameThread, equalTo(firstOtherThread));
    assertThat(sameThread, equalTo(secondOtherThread));
    assertThat(sameThread, not(theInstance(firstOtherThread)));
    assertThat(sameThread, not(theInstance(secondOtherThread)));
    assertThat(firstOtherThread, not(theInstance(secondOtherThread)));
  }

  private static class Record implements Serializable {
    private final double rand = Math.random();

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Record)) {
        return false;
      }
      Record that = (Record) other;
      return this.rand == that.rand;
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }
}
