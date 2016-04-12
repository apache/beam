package cz.seznam.euphoria.core.util;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * General utility class to store key/value pairs as strings providing converter
 * methods for primitives and frequently used types. Aims to help with presenting
 * a set of configuration/setting values.
 */
public class Settings implements Serializable {

  private Map<String, String> map = new ConcurrentHashMap<>();

  
  public Settings() {
  }

  public Settings(Settings conf) {
    putAll(conf);
  }

  public void putAll(Settings conf) {
    this.map.putAll(conf.map);
  }
  
  public void putAll(Map<String, String> map) {
    this.map.putAll(map);
  }

  public Map<String, String> getAll() {
    return map;
  }
  
  public boolean contains(String key) {
    return map.containsKey(requireNonNull(key));
  }

  // STRING ------------------------------------------------------------------------------
  public void setString(String key, String value) {
    map.put(requireNonNull(key), requireNonNull(value));
  }

  public String getString(String key, String def) {
    return map.containsKey(requireNonNull(key)) ? map.get(key) : def;
  }
  
  public String getString(String key) {
    if (!contains(key)) {
      throw new IllegalArgumentException("No value for: " + key);
    }
    return map.get(key);
  }

  // BOOL --------------------------------------------------------------------------------
  public void setBoolean(String key, boolean value) {
    setString(key, Boolean.toString(value));
  }

  public boolean getBoolean(String key, boolean def) {
    String stringVal = getString(key, null);
    return stringVal == null ? def :  Boolean.parseBoolean(stringVal);
  }
  
  public boolean getBoolean(String key) {
    String stringVal = getString(key);
    return Boolean.parseBoolean(stringVal);
  }

  // INT ---------------------------------------------------------------------------------
  public void setInt(String key, int value) {
    setString(key, String.valueOf(value));
  }
  
  public int getInt(String key, int def) {
    String stringVal = getString(key, null);
    return stringVal == null ? def : Integer.parseInt(stringVal);
  }
  
  public int getInt(String key) {
    String stringVal = getString(key);
    return Integer.parseInt(stringVal);
  }
  
  // LONG --------------------------------------------------------------------------------
  public void setLong(String key, long value) {
    setString(key, String.valueOf(value));
  }
  
  public long getLong(String key, long def) {
    String stringVal = getString(key, null);
    return stringVal == null ? def : Long.parseLong(stringVal);
  }
  
  public long getLong(String key) {
    String stringVal = getString(key);
    return Long.parseLong(stringVal);
  }

  // URI ---------------------------------------------------------------------------------
  public void setURI(String key, URI uri) {
    setString(key, uri.toString());
  }
  
  public URI getURI(String key, URI def) {
    String stringVal = getString(key, null);
    return stringVal == null ? def : URI.create(stringVal);
  }

  public URI getURI(String key) {
    String stringVal = getString(key);
    return URI.create(stringVal);
  }

  // CLASS -------------------------------------------------------------------------------
  public void setClass(String key, Class<?> cls) {
    setString(key, cls.getName());
  }

  public <E> Class<? extends E> getClass(String key, Class<E> superType) {
    String className = getString(key);
    return InstanceUtils.forName(className, superType);
  }
}
