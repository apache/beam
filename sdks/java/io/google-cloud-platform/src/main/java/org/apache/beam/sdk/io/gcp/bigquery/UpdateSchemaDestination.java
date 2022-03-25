package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

public class UpdateSchemaDestination extends DoFn<Iterable<KV<TableDestination, WriteTables.Result>>, TableDestination> {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateSchemaDestination.class);
    private final BigQueryServices bqServices;
    private final PCollectionView<String> jobIdToken;
    private final ValueProvider<String> loadJobProjectId;
    private transient @Nullable DatasetService datasetService;

    private static class PendingJobData {
        final BigQueryHelpers.PendingJob retryJob;
        final TableDestination tableDestination;
        final List<TableReference> tempTables;
        final BoundedWindow window;

        public PendingJobData(
                BigQueryHelpers.PendingJob retryJob,
                TableDestination tableDestination,
                List<TableReference> tempTables,
                BoundedWindow window) {
            this.retryJob = retryJob;
            this.tableDestination = tableDestination;
            this.tempTables = tempTables;
            this.window = window;
        }
    }

    private List<UpdateSchemaDestination.PendingJobData> pendingJobs = Lists.newArrayList();

    public UpdateSchemaDestination(BigQueryServices bqServices,
                                   PCollectionView<String> jobIdToken,
                                   ValueProvider<String> loadJobProjectId) {
        this.loadJobProjectId = loadJobProjectId;
        this.jobIdToken = jobIdToken;
        this.bqServices = bqServices;
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
        pendingJobs.clear();
    }

    @Teardown
    public void onTeardown() {

    }

    @ProcessElement
    public void processElement(
            @Element Iterable<KV<TableDestination, WriteTables.Result>> element,
            ProcessContext context
    ) {
        Multimap<TableDestination, WriteTables.Result> tempTables = ArrayListMultimap.create();
        for (KV<TableDestination, WriteTables.Result> entry : element) {
            tempTables.put(entry.getKey(), entry.getValue());
        }
    }

}
