/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.util;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Class that helps building and parsing (dropping) parameters to/from {@link URI}.
 * The user can freely read, add and drop query parameters of the underlying {@link URI}
 * and at the end one can build current state of the {@link URIParams} instance as a new
 * URI with {{@link #buildURI()}}. The string, int, long and boolean types of query
 * parameters are supported. The parameters are internally stored in a {@link Map}, so
 * duplicated parameters with the same name are not supported. The parameter keys are 
 * treated as case insensitive - i.e. there is no difference between {@code a=2} and
 * {@code A=3} and so you cannot parse uri like {@code http://my-site.org?a=2&A=2}
 * otherwise {@link java.lang.IllegalStateException} for duplicated parameter is thrown.
 * {@code null} values are also not supported.
 */
public class URIParams {

  private final URI uri;
  private final Map<Key, String> params;

  private URIParams(URI uri) {
    this.uri = uri;
    this.params = new LinkedHashMap<>();
    parseParams(uri);
  }
  
  /**
   * @param uri to work on
   * @return new {@link URIParams} instance
   */
  public static URIParams of(URI uri) {
    return new URIParams(uri);
  }

  private void parseParams(URI uri) {
    String q = uri.getRawQuery();
    if (q != null && !(q = q.trim()).isEmpty()) {
      String[] parts = q.split("&");
      for (String part : parts) {
        int i = part.indexOf('=');
        // ~ drop params without a value
        if (i != -1 && i != (part.length() - 1)) {
          setStringParam(
              URIUtil.paramDecode(part.substring(0, i)),
              URIUtil.paramDecode(part.substring(i + 1)));
        }
      }
    }
  }
  
  // / STRING ---------------------------------------------------------------------------
  /**
   * Sets {@link String}-valued query parameter. If the parameter name is already set
   * throws {@link IllegalArgumentException}.
   * @param name of the query parameter
   * @param value of the parameter
   * @return this {@link URIParams} instance to support chain building.
   * @throws IllegalStateException if name or value is empty or parameter is already 
   *         set
   */
  public URIParams setStringParam(String name, String value) {
    if (isEmpty(name)) {
      throw new IllegalArgumentException("Empty names are not supported!");
    }
    if (isEmpty(value)) {
      throw new IllegalArgumentException("Empty values are not supported! Name: " + name);
    }
    String prev = put(name, value);
    if (prev != null) {
      throw new IllegalStateException(String.format("%s already set in URI: %s",
          new Object[] { name, uri.toString() }));
    }
    return this;
  }

  /**
   * Sets {@link String}-valued query parameters. If the parameter name is already set
   * throws {@link IllegalArgumentException}. Empty keys or values are ignored.
   * 
   * @param params map of parameters
   * @return this {@link URIParams} instance to support chain building.
   * @throws IllegalArgumentException if parameter is already set
   */
  public URIParams setStringParams(Map<String, String> params) {
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();
      // add only non-empty keys and values
      if (!isEmpty(name) && !isEmpty(value)) {
        setStringParam(name, value);
      }
    }
    return this;
  }

  /**
   * Returns the value of the param as {@link String}. Throws
   * {@link IllegalArgumentException} if the parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as {@link String}
   * @throws IllegalArgumentException if parameter is not set
   */
  public String getStringParam(String name) {
    String string = get(name);
    if (string == null) {
      throw new NoSuchElementException(String.format("No parameter for: %s in %s.",
          name, uri.toString()));
    }
    return string;
  }

  /**
   * Returns the value of the parameter as {@link String}. If the parameter with the given
   * name does not exist, returns the given default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return query parameter value as {@link String} or default value if not present
   */
  public String getStringParam(String name, String _default) {
    String string = get(name);
    if (string == null) {
      return _default;
    }
    return string;
  }

  /**
   * Returns the parameter value as {@link String} and drops the parameter if present.
   * Throws {@link IllegalArgumentException} if the parameter with the given name does not
   * exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as {@link String}
   * @throws IllegalArgumentException if parameter is not set
   */
  public String getAndDropStringParam(String name) {
    String string = getStringParam(name);
    dropParam(name);
    return string;
  }

  /**
   * Returns the parameter value as {@link String} and drops the parameter if present.
   * Otherwise return given _default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return parameter value or _default if not present
   */
  public String getAndDropStringParam(String name, String _default) {
    String string = get(name);
    if (isEmpty(string)) {
      return _default;
    }
    dropParam(name);
    return string;
  }

  // / INT ------------------------------------------------------------------------------
  /**
   * Sets integer-valued query parameter. If the parameter name is already set throws
   * {@link IllegalArgumentException}.
   * 
   * @param name of the query parameter
   * @param value of the parameter
   * @return this {@link URIParams} instance to support chain building.
   * @throws IllegalArgumentException if parameter is already set
   */
  public URIParams setIntParam(String name, int value) {
    setStringParam(name, String.valueOf(value));
    return this;
  }

  /**
   * Returns the value of the parameter as integer. Throws
   * {@link IllegalArgumentException} if the parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as integer
   * @throws IllegalArgumentException if parameter is not set
   * @throws NumberFormatException if the parameter cannot be parsed to integer
   */
  public int getIntParam(String name) {
    String string = getStringParam(name);
    return toInt(name, string);
  }

  /**
   * Returns the value of the parameter as integer. If the parameter with the given name
   * does not exist, returns the given default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return query parameter value as {@link String} or default value if not present
   * @throws NumberFormatException if the parameter cannot be parsed to integer
   */
  public int getIntParam(String name, int _default) {
    String string = getStringParam(name, null);
    if (string == null) {
      return _default;
    }
    return toInt(name, string);
  }

  /**
   * Returns the parameter value as integer and drops the parameter if present. Throws
   * {@link IllegalArgumentException} if the parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as integer
   * @throws IllegalArgumentException if parameter is not set
   * @throws NumberFormatException if the parameter cannot be parsed to integer
   */
  public int getAndDropIntParam(String name) {
    String string = getAndDropStringParam(name);
    return toInt(name, string);
  }

  /**
   * Returns the parameter value as integer and drops the parameter if present. Otherwise
   * return given _default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return parameter value or _default if not present
   * @throws NumberFormatException if the parameter cannot be parsed to integer
   */
  public int getAndDropIntParam(String name, int _default) {
    String string = getAndDropStringParam(name, null);
    if (string == null) {
      return _default;
    }
    return toInt(name, string);
  }

  // / LONG -----------------------------------------------------------------------------
  /**
   * Sets long-valued query parameter. If the parameter name is already set throws
   * {@link IllegalArgumentException}.
   * 
   * @param name of the query parameter
   * @param value of the parameter
   * @return this {@link URIParams} instance to support chain building.
   * @throws IllegalArgumentException if parameter is already set
   */
  public URIParams setLongParam(String name, long value) {
    setStringParam(name, String.valueOf(value));
    return this;
  }

  /**
   * Returns the value of the parameter as long. Throws {@link IllegalArgumentException}
   * if the parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as long
   * @throws IllegalArgumentException if parameter is not set
   * @throws NumberFormatException if the parameter cannot be parsed to long
   */
  public long getLongParam(String name) {
    String string = getStringParam(name);
    return toLong(name, string);
  }

  /**
   * Returns the value of the parameter as long. If the parameter with the given name does
   * not exist, returns the given default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return query parameter value as long or default value if not present
   * @throws NumberFormatException if the parameter cannot be parsed to long
   */
  public long getLongParam(String name, long _default) {
    String string = getStringParam(name, null);
    if (string == null) {
      return _default;
    }
    return toLong(name, string);
  }

  /**
   * Returns the parameter value as long and drops the parameter if present. Throws
   * {@link IllegalArgumentException} if the parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as long
   * @throws IllegalArgumentException if parameter is not set
   * @throws NumberFormatException if the parameter cannot be parsed to long
   */
  public long getAndDropLongParam(String name) {
    String string = getAndDropStringParam(name);
    return toLong(name, string);
  }

  /**
   * Returns the parameter value as long and drops the parameter if present. Otherwise
   * return given _default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return parameter value or _default if not present
   * @throws NumberFormatException if the parameter cannot be parsed to long
   */
  public long getAndDropLongParam(String name, long _default) {
    String string = getAndDropStringParam(name, null);
    if (string == null) {
      return _default;
    }
    return toLong(name, string);
  }

  // / BOOLEAN --------------------------------------------------------------------------
  /**
   * Sets boolean-valued query parameter. If the parameter name is already set throws
   * {@link IllegalStateException}.
   * 
   * @param name of the query parameter
   * @param value of the parameter
   * @return this {@link URIParams} instance to support chain building.
   * @throws IllegalStateException if parameter is already set
   */
  public URIParams setBoolParam(String name, boolean value) {
    setStringParam(name, String.valueOf(value));
    return this;
  }

  /**
   * Returns the value of the parameter as boolean (uses
   * {@link Boolean#parseBoolean(String)}). Throws {@link IllegalArgumentException} if the
   * parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as long
   * @throws IllegalArgumentException if parameter is not set
   */
  public boolean getBoolParam(String name) {
    String string = getStringParam(name);
    return toBool(name, string);
  }

  /**
   * Returns the value of the parameter as boolean (uses
   * {@link Boolean#parseBoolean(String)}). If the parameter with the given name does not
   * exist, returns the given default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return query parameter value as long or default value if not present
   */
  public boolean getBoolParam(String name, boolean _default) {
    String string = getStringParam(name, null);
    if (string == null) {
      return _default;
    }
    return toBool(name, string);
  }

  /**
   * Returns the parameter value as boolean (uses {@link Boolean#parseBoolean(String)})
   * and drops the parameter if present. Throws {@link IllegalArgumentException} if the
   * parameter with the given name does not exist.
   * 
   * @param name of the query parameter name
   * @return query parameter value as boolean
   * @throws IllegalArgumentException if parameter is not set
   */
  public boolean getAndDropBoolParam(String name) {
    String string = getAndDropStringParam(name);
    return toBool(name, string);
  }

  /**
   * Returns the parameter value as boolean (uses {@link Boolean#parseBoolean(String)})
   * and drops the parameter if present. Otherwise return given _default value.
   * 
   * @param name of the query parameter name
   * @param _default value if the parameter is not present
   * @return parameter value or _default if not present
   */
  public boolean getAndDropBoolParam(String name, boolean _default) {
    String string = getAndDropStringParam(name, null);
    if (string == null) {
      return _default;
    }
    return toBool(name, string);
  }

  /**
   * Drops all query parameters.
   * 
   * @return string to string map of all query parameters
   */
  public Map<String, String> dropAllParams() {
    Map<String, String> ret = new LinkedHashMap<>();
    for (Map.Entry<Key, String> entry: params.entrySet()) {
      ret.put(entry.getKey().value, entry.getValue());
    }
    params.clear();
    return ret;
  }

  /**
   * @param name of the query parameter to be dropped
   */
  public void dropParam(String name) {
    String value = remove(name);
    if (value == null) {
      throw new IllegalArgumentException(String.format(
          "Dropping parameter \"%s\" which does not exist.", name));
    }
  }

  /**
   * @return new {@link URI} with actual query parameters
   */
  public URI buildURI() {
    return URIUtil.build(
        uri.getScheme(), 
        uri.getRawAuthority(), 
        uri.getRawPath(),
        buildQueryString(), 
        uri.getRawFragment());
  }

  private String buildQueryString() {
    if (params.isEmpty()) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<Key, String> entry : params.entrySet()) {
      if (builder.length() > 0) {
        builder.append("&");
      }
      builder
          .append(URIUtil.paramEncode(entry.getKey().value))
          .append("=")
          .append(URIUtil.paramEncode(entry.getValue()));
    }
    return builder.toString();
  }

  // / PRIVATES -------------------------------------------------------------------------
  private boolean isEmpty(String string) {
    return string == null || string.isEmpty();
  }

  private int toInt(String name, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(parseExceptionString(name, value, "integer"));
    }
  }

  private long toLong(String name, String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(parseExceptionString(name, value, "long"));
    }
  }

  private boolean toBool(String name, String value) {
    if (value.equalsIgnoreCase("true")) {
      return true;
    }
    if (value.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException(parseExceptionString(name, value, "boolean"));
  }

  private String parseExceptionString(String name, String value, String type) {
    return String.format("%s param's value %s cannot be parsed to %s!", name, value, type);
  }
  
  private String put(String key, String value) {
    return params.put(new Key(key), value);
  }
  
  private String get(String key) {
    return params.get(new Key(key));
  }
  
  private String remove(String key) {
    return params.remove(new Key(key));
  }
  
  private static class Key {
    private final String value;
    private final String lowered;
    public Key(String value) {
      this.value = value;
      this.lowered = value.toLowerCase(Locale.ENGLISH);
    }
    @Override
    public int hashCode() {
      return lowered.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Key) {
        Key other = (Key) obj;
        return lowered.equals(other.lowered);
      }
      return false;
    }
  }
}
