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
package org.apache.beam.sdk.io.clickhouse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * Utility class for parsing ClickHouse JDBC URLs and extracting connection parameters.
 *
 * <p>Used for supporting backward compatibility with the deprecated {@link
 * ClickHouseIO#write(String, String)} method that accepts JDBC URLs. New code should use {@link
 * ClickHouseIO#write(String, String, String)} with explicit parameters instead.
 *
 * @deprecated Use {@link ClickHouseIO#write(String, String, String)} with separate clickHouseUrl,
 *     database, and table parameters instead of JDBC URL format.
 */
@Deprecated
class ClickHouseJdbcUrlParser {

  /**
   * Represents parsed components of a ClickHouse JDBC URL.
   *
   * <p>Contains the extracted HTTP/HTTPS URL, database name, and connection properties from a JDBC
   * URL string.
   *
   * @deprecated This class supports the deprecated JDBC URL-based API. Use separate parameters for
   *     clickHouseUrl, database, and properties instead.
   */
  @Deprecated
  static class ParsedJdbcUrl {
    private final String clickHouseUrl;
    private final String database;
    private final Properties properties;

    ParsedJdbcUrl(String clickHouseUrl, String database, Properties properties) {
      this.clickHouseUrl = clickHouseUrl;
      this.database = database;
      this.properties = properties;
    }

    public String getClickHouseUrl() {
      return clickHouseUrl;
    }

    public String getDatabase() {
      return database;
    }

    public Properties getProperties() {
      return properties;
    }
  }

  /**
   * Parses a ClickHouse JDBC URL into its components.
   *
   * <p>Supported formats:
   *
   * <ul>
   *   <li>jdbc:clickhouse://host:port/database?param=value
   *   <li>jdbc:clickhouse:http://host:port/database?param=value
   *   <li>jdbc:clickhouse:https://host:port/database?param=value
   *   <li>jdbc:ch://host:port/database?param=value (ClickHouse JDBC driver shorthand)
   * </ul>
   *
   * @param jdbcUrl the JDBC URL to parse
   * @return ParsedJdbcUrl containing the HTTP/HTTPS URL, database, and properties
   * @throws IllegalArgumentException if the URL format is invalid
   */
  static ParsedJdbcUrl parse(String jdbcUrl) {
    if (Strings.isNullOrEmpty(jdbcUrl)) {
      throw new IllegalArgumentException("JDBC URL cannot be null or empty");
    }

    String actualUrl = extractHttpUrl(jdbcUrl);

    try {
      URI uri = new URI(actualUrl);

      validateScheme(uri.getScheme());
      String host = validateAndGetHost(uri.getHost(), jdbcUrl);
      int port = getPortOrDefault(uri.getPort(), uri.getScheme());

      String clickHouseUrl = String.format("%s://%s:%d", uri.getScheme(), host, port);
      String database = extractDatabase(uri.getPath());
      Properties properties = extractProperties(uri.getQuery());

      return new ParsedJdbcUrl(clickHouseUrl, database, properties);

    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid JDBC URL format: " + jdbcUrl, e);
    } catch (java.io.UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Failed to decode URL parameters: " + jdbcUrl, e);
    }
  }

  /**
   * Extracts and normalizes the HTTP/HTTPS URL from a JDBC URL.
   *
   * @param jdbcUrl the JDBC URL to process
   * @return normalized HTTP/HTTPS URL
   * @throws IllegalArgumentException if the URL format is invalid
   */
  private static String extractHttpUrl(String jdbcUrl) {
    // Remove jdbc: prefix
    String urlWithoutJdbc = jdbcUrl;
    if (jdbcUrl.toLowerCase().startsWith("jdbc:")) {
      urlWithoutJdbc = jdbcUrl.substring(5);
    }

    // Handle jdbc:clickhouse: or jdbc:ch: prefix
    String actualUrl;
    if (urlWithoutJdbc.toLowerCase().startsWith("clickhouse:")) {
      actualUrl = urlWithoutJdbc.substring(11);
    } else if (urlWithoutJdbc.toLowerCase().startsWith("ch:")) {
      actualUrl = urlWithoutJdbc.substring(3);
    } else {
      throw new IllegalArgumentException(
          "Invalid JDBC URL format. Expected 'jdbc:clickhouse:' or 'jdbc:ch:' prefix. Got: "
              + jdbcUrl);
    }

    // Check if URL already has a scheme and validate it
    if (actualUrl.toLowerCase().startsWith("http://")
        || actualUrl.toLowerCase().startsWith("https://")) {
      return actualUrl;
    }

    // Check for invalid schemes before prepending http://
    if (actualUrl.contains("://")) {
      // Extract the scheme part
      int schemeEnd = actualUrl.indexOf("://");
      String scheme = actualUrl.substring(0, schemeEnd).toLowerCase();
      if (!scheme.equals("http") && !scheme.equals("https")) {
        throw new IllegalArgumentException(
            "Invalid scheme in JDBC URL. Expected 'http' or 'https'. Got: " + scheme);
      }
    }

    // If URL doesn't start with http:// or https://, assume http://
    if (actualUrl.startsWith("//")) {
      actualUrl = "http:" + actualUrl;
    } else {
      actualUrl = "http://" + actualUrl;
    }

    return actualUrl;
  }

  /**
   * Validates the URI scheme.
   *
   * @param scheme the scheme to validate
   * @throws IllegalArgumentException if scheme is invalid
   */
  private static void validateScheme(String scheme) {
    if (scheme == null || (!scheme.equals("http") && !scheme.equals("https"))) {
      throw new IllegalArgumentException(
          "Invalid scheme. Expected 'http' or 'https'. Got: " + scheme);
    }
  }

  /**
   * Validates and returns the host from the URI.
   *
   * @param host the host to validate
   * @param jdbcUrl the original JDBC URL (for error reporting)
   * @return the validated host
   * @throws IllegalArgumentException if host is invalid
   */
  private static String validateAndGetHost(String host, String jdbcUrl) {
    if (Strings.isNullOrEmpty(host)) {
      throw new IllegalArgumentException("Host cannot be empty in JDBC URL: " + jdbcUrl);
    }
    return host;
  }

  /**
   * Returns the port or default port based on scheme.
   *
   * @param port the port from URI (-1 if not specified)
   * @param scheme the URI scheme (http or https)
   * @return the port number
   */
  private static int getPortOrDefault(int port, String scheme) {
    if (port == -1) {
      return scheme.equals("https") ? 8443 : 8123; // Default ClickHouse ports
    }
    return port;
  }

  /**
   * Extracts database name from URI path.
   *
   * @param path the URI path
   * @return the database name, or "default" if not specified
   */
  private static String extractDatabase(String path) {
    if (Strings.isNullOrEmpty(path)) {
      return "default";
    }

    // Remove leading slash
    String pathWithoutSlash = path.startsWith("/") ? path.substring(1) : path;
    return pathWithoutSlash.isEmpty() ? "default" : pathWithoutSlash;
  }

  /**
   * Extracts connection properties from URI query string.
   *
   * @param query the URI query string
   * @return Properties object containing the parsed parameters
   * @throws java.io.UnsupportedEncodingException if URL decoding fails
   */
  private static Properties extractProperties(String query)
      throws java.io.UnsupportedEncodingException {
    Properties properties = new Properties();

    if (Strings.isNullOrEmpty(query)) {
      return properties;
    }

    // Use Guava Splitter instead of String.split()
    for (String param : Splitter.on('&').split(query)) {
      // Split key-value pairs, handling parameters without values
      List<String> parts = Splitter.on('=').limit(2).splitToList(param);

      if (parts.size() == 2) {
        String key = java.net.URLDecoder.decode(parts.get(0), "UTF-8");
        String value = java.net.URLDecoder.decode(parts.get(1), "UTF-8");
        properties.setProperty(key, value);
      } else if (parts.size() == 1) {
        // Parameter without value (e.g., ?compress)
        String key = java.net.URLDecoder.decode(parts.get(0), "UTF-8");
        properties.setProperty(key, "true");
      }
    }

    return properties;
  }
}
