package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.Watch;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FileIO_MatchConfiguration extends FileIO.MatchConfiguration {

  private final EmptyMatchTreatment emptyMatchTreatment;

  private final boolean matchUpdatedFiles;

  private final @Nullable Duration watchInterval;

  private final Watch.Growth.@Nullable TerminationCondition<String, ?> watchTerminationCondition;

  private AutoValue_FileIO_MatchConfiguration(
      EmptyMatchTreatment emptyMatchTreatment,
      boolean matchUpdatedFiles,
      @Nullable Duration watchInterval,
      Watch.Growth.@Nullable TerminationCondition<String, ?> watchTerminationCondition) {
    this.emptyMatchTreatment = emptyMatchTreatment;
    this.matchUpdatedFiles = matchUpdatedFiles;
    this.watchInterval = watchInterval;
    this.watchTerminationCondition = watchTerminationCondition;
  }

  @Override
  public EmptyMatchTreatment getEmptyMatchTreatment() {
    return emptyMatchTreatment;
  }

  @Override
  public boolean getMatchUpdatedFiles() {
    return matchUpdatedFiles;
  }

  @Override
  public @Nullable Duration getWatchInterval() {
    return watchInterval;
  }

  @Override
  Watch.Growth.@Nullable TerminationCondition<String, ?> getWatchTerminationCondition() {
    return watchTerminationCondition;
  }

  @Override
  public String toString() {
    return "MatchConfiguration{"
        + "emptyMatchTreatment=" + emptyMatchTreatment + ", "
        + "matchUpdatedFiles=" + matchUpdatedFiles + ", "
        + "watchInterval=" + watchInterval + ", "
        + "watchTerminationCondition=" + watchTerminationCondition
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FileIO.MatchConfiguration) {
      FileIO.MatchConfiguration that = (FileIO.MatchConfiguration) o;
      return this.emptyMatchTreatment.equals(that.getEmptyMatchTreatment())
          && this.matchUpdatedFiles == that.getMatchUpdatedFiles()
          && (this.watchInterval == null ? that.getWatchInterval() == null : this.watchInterval.equals(that.getWatchInterval()))
          && (this.watchTerminationCondition == null ? that.getWatchTerminationCondition() == null : this.watchTerminationCondition.equals(that.getWatchTerminationCondition()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= emptyMatchTreatment.hashCode();
    h$ *= 1000003;
    h$ ^= matchUpdatedFiles ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (watchInterval == null) ? 0 : watchInterval.hashCode();
    h$ *= 1000003;
    h$ ^= (watchTerminationCondition == null) ? 0 : watchTerminationCondition.hashCode();
    return h$;
  }

  @Override
  FileIO.MatchConfiguration.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FileIO.MatchConfiguration.Builder {
    private EmptyMatchTreatment emptyMatchTreatment;
    private Boolean matchUpdatedFiles;
    private @Nullable Duration watchInterval;
    private Watch.Growth.@Nullable TerminationCondition<String, ?> watchTerminationCondition;
    Builder() {
    }
    private Builder(FileIO.MatchConfiguration source) {
      this.emptyMatchTreatment = source.getEmptyMatchTreatment();
      this.matchUpdatedFiles = source.getMatchUpdatedFiles();
      this.watchInterval = source.getWatchInterval();
      this.watchTerminationCondition = source.getWatchTerminationCondition();
    }
    @Override
    FileIO.MatchConfiguration.Builder setEmptyMatchTreatment(EmptyMatchTreatment emptyMatchTreatment) {
      if (emptyMatchTreatment == null) {
        throw new NullPointerException("Null emptyMatchTreatment");
      }
      this.emptyMatchTreatment = emptyMatchTreatment;
      return this;
    }
    @Override
    FileIO.MatchConfiguration.Builder setMatchUpdatedFiles(boolean matchUpdatedFiles) {
      this.matchUpdatedFiles = matchUpdatedFiles;
      return this;
    }
    @Override
    FileIO.MatchConfiguration.Builder setWatchInterval(Duration watchInterval) {
      this.watchInterval = watchInterval;
      return this;
    }
    @Override
    FileIO.MatchConfiguration.Builder setWatchTerminationCondition(Watch.Growth.TerminationCondition<String, ?> watchTerminationCondition) {
      this.watchTerminationCondition = watchTerminationCondition;
      return this;
    }
    @Override
    FileIO.MatchConfiguration build() {
      if (this.emptyMatchTreatment == null
          || this.matchUpdatedFiles == null) {
        StringBuilder missing = new StringBuilder();
        if (this.emptyMatchTreatment == null) {
          missing.append(" emptyMatchTreatment");
        }
        if (this.matchUpdatedFiles == null) {
          missing.append(" matchUpdatedFiles");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FileIO_MatchConfiguration(
          this.emptyMatchTreatment,
          this.matchUpdatedFiles,
          this.watchInterval,
          this.watchTerminationCondition);
    }
  }

}
