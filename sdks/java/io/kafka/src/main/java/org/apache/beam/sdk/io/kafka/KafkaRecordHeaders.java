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
package org.apache.beam.sdk.io.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.utils.AbstractIterator;

/**
 * This is a copy of Kafka's {@link org.apache.kafka.common.header.internals.RecordHeaders}.
 * Included here in order to support older Kafka versions (0.9.x).
 */
public class KafkaRecordHeaders implements KafkaHeaders {
  private final List<KafkaHeader> headers;
  private volatile boolean isReadOnly;

  public KafkaRecordHeaders() {
    this((Iterable<KafkaHeader>) null);
  }

  public KafkaRecordHeaders(KafkaHeader[] headers) {
    if (headers == null) {
      this.headers = new ArrayList<>();
    } else {
      this.headers = new ArrayList<>(Arrays.asList(headers));
    }
  }

  public KafkaRecordHeaders(Iterable<KafkaHeader> headers) {
    // Use efficient copy constructor if possible, fallback to iteration otherwise
    if (headers == null) {
      this.headers = new ArrayList<>();
    } else if (headers instanceof KafkaRecordHeaders) {
      this.headers = new ArrayList<>(((KafkaRecordHeaders) headers).headers);
    } else if (headers instanceof Collection) {
      this.headers = new ArrayList<>((Collection<KafkaHeader>) headers);
    } else {
      this.headers = new ArrayList<>();
      for (KafkaHeader header : headers) {
        this.headers.add(header);
      }
    }
  }

  @Override
  public KafkaHeaders add(KafkaHeader header) throws IllegalStateException {
    canWrite();
    headers.add(header);
    return this;
  }

  public KafkaHeaders add(String key, byte[] value) throws IllegalStateException {
    return add(new KafkaRecordHeader(key, value));
  }

  public KafkaHeaders remove(String key) throws IllegalStateException {
    canWrite();
    checkKey(key);
    Iterator<KafkaHeader> iterator = iterator();
    while (iterator.hasNext()) {
      if (iterator.next().key().equals(key)) {
        iterator.remove();
      }
    }
    return this;
  }

  public KafkaHeader lastHeader(String key) {
    checkKey(key);
    for (int i = headers.size() - 1; i >= 0; i--) {
      KafkaHeader header = headers.get(i);
      if (header.key().equals(key)) {
        return header;
      }
    }
    return null;
  }

  public Iterable<KafkaHeader> headers(final String key) {
    checkKey(key);
    return new Iterable<KafkaHeader>() {
      @Override
      public Iterator<KafkaHeader> iterator() {
        return new KafkaRecordHeaders.FilterByKeyIterator(headers.iterator(), key);
      }
    };
  }

  public Iterator<KafkaHeader> iterator() {
    return closeAware(headers.iterator());
  }

  public void setReadOnly() {
    this.isReadOnly = true;
  }

  public KafkaHeader[] toArray() {
    return headers.isEmpty()
        ? new KafkaHeader[0]
        : headers.toArray(new KafkaHeader[headers.size()]);
  }

  private void checkKey(String key) {
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null.");
    }
  }

  private void canWrite() {
    if (isReadOnly) {
      throw new IllegalStateException("RecordHeaders has been closed.");
    }
  }

  private Iterator<KafkaHeader> closeAware(final Iterator<KafkaHeader> original) {
    return new Iterator<KafkaHeader>() {
      @Override
      public boolean hasNext() {
        return original.hasNext();
      }

      public KafkaHeader next() {
        return original.next();
      }

      @Override
      public void remove() {
        canWrite();
        original.remove();
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaRecordHeaders headers1 = (KafkaRecordHeaders) o;

    return headers != null ? headers.equals(headers1.headers) : headers1.headers == null;
  }

  @Override
  public int hashCode() {
    return headers != null ? headers.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "RecordHeaders(" + "headers = " + headers + ", isReadOnly = " + isReadOnly + ')';
  }

  private static final class FilterByKeyIterator extends AbstractIterator<KafkaHeader> {

    private final Iterator<KafkaHeader> original;
    private final String key;

    private FilterByKeyIterator(Iterator<KafkaHeader> original, String key) {
      this.original = original;
      this.key = key;
    }

    protected KafkaHeader makeNext() {
      while (true) {
        if (original.hasNext()) {
          KafkaHeader header = original.next();
          if (!header.key().equals(key)) {
            continue;
          }

          return header;
        }
        return this.allDone();
      }
    }
  }
}
