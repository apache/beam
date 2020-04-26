package org.apache.beam.sdk.testutils.publishing;

import org.apache.beam.sdk.testutils.NamedTestResult;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Base64;

import static java.util.Objects.requireNonNull;

public final class InfluxDBPublisher {

    private InfluxDBPublisher() {
    }

    public static void publishWithSettings(final Collection<NamedTestResult> results,
                                              final InfluxDBSettings settings) {
        requireNonNull(settings, "InfluxDB settings must not be null");
        try {
            publish(results, settings);
        } catch (final Exception exception) {
            throw new InfluxDBException(exception);
        }
    }

    private static void publish(final Collection<NamedTestResult> results,
                                   final InfluxDBSettings settings) throws Exception {
        final HttpURLConnection connection =
                (HttpURLConnection) new URL(settings.host + "?db=" + settings.database).openConnection();

        try (final AutoCloseable ignored = connection::disconnect;
             final DataOutputStream stream = new DataOutputStream(connection.getOutputStream())) {

            connection.setDoOutput(true);
            connection.setRequestMethod("POST");

            connection.setRequestProperty("Authorization", getTokenAsString(settings.userName, settings.userPassword));

            final StringBuilder builder = new StringBuilder();
            results.stream()
                    .map(NamedTestResult::toMap)
                    .forEach(map -> builder
                            .append(settings.measurement).append(",")
                            .append("test_id").append("=").append(map.get("test_id")).append(",")
                            .append("metric").append("=").append(map.get("metric")).append(" ")
                            .append("value").append("=").append(map.get("value")).append('\n'));

            stream.writeBytes(builder.toString());
            stream.flush();

            is2xx(connection.getResponseCode());
        }
    }

    private static String getTokenAsString(final String user, final String password) {
        final String auth = user + ":" + password;
        final byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return  "Basic " + new String(encodedAuth);
    }

    private static void is2xx(final int code) throws IOException {
        if (code < 200 || code >= 300) {
            throw new IOException();
        }
    }

    private static class InfluxDBException extends RuntimeException {
        public InfluxDBException(final Exception exception) {
            super("Unable to write metrics", exception);
        }
    }
}