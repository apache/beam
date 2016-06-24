package cz.seznam.euphoria.core.client.dataset;


import java.time.Duration;

// TODO Will be replaced by Duration
@Deprecated
public class After {
  Duration period;

  private After(Duration period) {
    this.period = period;
  }

  public static After seconds(long seconds) {
    return new After(Duration.ofSeconds(seconds));
  }

  public static After minutes(long minutes) {
    return new After(Duration.ofMinutes(minutes));
  }

  public static After hours(long hours) {
    return new After(Duration.ofHours(hours));
  }
}