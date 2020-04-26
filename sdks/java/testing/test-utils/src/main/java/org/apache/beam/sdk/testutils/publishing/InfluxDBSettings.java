package org.apache.beam.sdk.testutils.publishing;

import java.util.Arrays;
import java.util.Objects;

public final class InfluxDBSettings {

    public final String host;
    public final String userName;
    public final String userPassword;
    public final String measurement;
    public final String database;

    private InfluxDBSettings(String host, String userName, String userPassword, String measurement, String database) {
        this.host = host;
        this.userName = userName;
        this.userPassword = userPassword;
        this.measurement = measurement;
        this.database = database;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private String userName;
        private String userPassword;
        private String measurement;
        private String database;

        public Builder withHost(final String host) {
            this.host = host;
            return this;
        }

        public Builder withUserName(final String userName) {
            this.userName = userName;
            return this;
        }

        public Builder withUserPassword(final String userPassword) {
            this.userPassword = userPassword;
            return this;
        }

        public Builder withMeasurement(final String measurement) {
            this.measurement = measurement;
            return this;
        }

        public Builder withDatabase(final String database) {
            this.database = database;
            return this;
        }

        public InfluxDBSettings get() {
            allNotNull(host, userName, userPassword, measurement, database);
            return new InfluxDBSettings(
                    host,
                    userName,
                    userPassword,
                    measurement,
                    database
            );
        }

        private void allNotNull(Object...objects) {
            Arrays.stream(objects).forEach(Objects::requireNonNull);
        }
    }
}
