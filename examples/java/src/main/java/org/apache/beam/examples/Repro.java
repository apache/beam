package org.apache.beam.examples;

import com.google.api.services.bigquery.model.Clustering;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class Repro {

    public static void main(String[] args) {
        Clustering clustering = new Clustering().setFields(ImmutableList.of("date", "time"));

        System.out.println("CLUSTERING: " + clustering);
        System.out.println("JSON CLUSTERING: " + BigQueryHelpers.toJsonString(clustering));
    }
}
