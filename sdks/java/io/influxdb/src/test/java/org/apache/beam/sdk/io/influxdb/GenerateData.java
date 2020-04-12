package org.apache.beam.sdk.io.influxdb;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

class GenerateData {

  private static Random random = new Random();

  static List<String> getMetric(int number) {
    List<String> element = new ArrayList<>();
    for (int i = 0; i < number; i++) {
      Model m = new Model();
      m.setMeasurement("test_m");
      m.addField("test1", random.nextInt(100));
      m.addField("test2", random.nextInt(100));
      LocalDateTime time =
          LocalDateTime.of(
              LocalDate.now(),
              LocalTime.of(
                  random.nextInt(24),
                  random.nextInt(60),
                  random.nextInt(60),
                  random.nextInt(999999999 + 1)));
      ZonedDateTime zdt = time.atZone(ZoneId.of("America/Los_Angeles"));
      m.setTime(zdt.toInstant().toEpochMilli());
      m.setTimeUnit(TimeUnit.MILLISECONDS);
      element.add(m.getLineProtocol());
    }
    return element;
  }

  static List<String> getMultipleMetric(int number) {
    List<String> element = new ArrayList<>();
    for (int i = 0; i < number; i++) {
      Model m = new Model();
      Model m1 = new Model();
      m.setMeasurement("test_m");
      m.addField("test1", random.nextInt(100));
      m.addField("test2", random.nextInt(100));
      m1.setMeasurement("test_m1");
      m1.addField("test1", random.nextInt(100));
      m1.addField("test2", random.nextInt(100));
      LocalDateTime time =
          LocalDateTime.of(
              LocalDate.now(),
              LocalTime.of(
                  random.nextInt(24),
                  random.nextInt(60),
                  random.nextInt(60),
                  random.nextInt(999999999 + 1)));
      ZonedDateTime zdt = time.atZone(ZoneId.of("America/Los_Angeles"));
      m.setTime(zdt.toInstant().toEpochMilli());
      m.setTimeUnit(TimeUnit.MILLISECONDS);
      m1.setTime(zdt.toInstant().toEpochMilli());
      m1.setTimeUnit(TimeUnit.MILLISECONDS);
      element.add(m.getLineProtocol());
      element.add(m1.getLineProtocol());
    }
    return element;
  }
}
