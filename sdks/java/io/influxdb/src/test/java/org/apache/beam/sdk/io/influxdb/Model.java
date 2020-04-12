package org.apache.beam.sdk.io.influxdb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.influxdb.dto.Point;

@DefaultCoder(AvroCoder.class)
class Model implements LineProtocolConvertable {
  private String measurement;
  private Map<String, String> tags;
  private Map<String, Object> fields;
  private Long time;
  private TimeUnit timeUnit;

  Model() {
    tags = new HashMap<>();
    fields = new HashMap<>();
  }

  public Long getTime() {
    return time;
  }

  public void setTime(Long time) {
    this.time = time;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  public void addField(String name, Object value) {
    fields.put(name, value);
  }

  public void addTags(String name, String value) {
    tags.put(name, value);
  }

  public String getMeasurement() {
    return measurement;
  }

  public void setMeasurement(String measurement) {
    this.measurement = measurement;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public Map<String, Object> getFields() {
    return fields;
  }

  public void setFields(Map<String, Object> fields) {
    this.fields = fields;
  }

  @Override
  public String getLineProtocol() {
    return Point.measurement(measurement)
        .time(time, timeUnit)
        .fields(fields)
        .tag(tags)
        .build()
        .lineProtocol();
  }
}
