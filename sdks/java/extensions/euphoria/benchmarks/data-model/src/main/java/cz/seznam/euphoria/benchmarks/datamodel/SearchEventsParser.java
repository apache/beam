/**
 * Copyright 2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.benchmarks.datamodel;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class SearchEventsParser implements Serializable {

  public static class Query implements Serializable {
    public long timestamp;
    public String query;

    public Query() {}

    private Query(long timestamp, String query) {
      this.timestamp = timestamp;
      this.query = query;
    }

    @Override
    public String toString() {
      return "Query{" +
              "timestamp=" + timestamp +
              ", query='" + query + '\'' +
              '}';
    }
  }

  private static final Locale CS = new Locale("cs", "CZ");

  // ~ returns {@code null} if given message does not represent a query.
  public Query parse(byte [] message) throws Exception {
    return parse(new String(message, StandardCharsets.UTF_8));
  }

  public Query parse(String s) throws IOException {
    if (s == null || (s = s.trim()).isEmpty()) {
      return null;
    }

    String[] split = s.split("\t");
    if (split.length < 2) {
      return null;
    }

    long timestamp = Long.parseLong(split[0]);
    String query = StringUtils.stripAccents(split[1].toLowerCase(CS));
    return new Query(timestamp, query);
  }
}
