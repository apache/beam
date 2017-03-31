package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.fromJsonString;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.options.BigQueryOptions;


/**
 * Created by relax on 3/30/17.
 */
class FakeBigQueryServices implements BigQueryServices {
  private String[] jsonTableRowReturns = new String[0];
  private JobService jobService;
  private DatasetService datasetService;

  public FakeBigQueryServices withJobService(JobService jobService) {
    this.jobService = jobService;
    return this;
  }

  public FakeBigQueryServices withDatasetService(DatasetService datasetService) {
    this.datasetService = datasetService;
    return this;
  }

  public FakeBigQueryServices readerReturns(String... jsonTableRowReturns) {
    this.jsonTableRowReturns = jsonTableRowReturns;
    return this;
  }

  @Override
  public JobService getJobService(BigQueryOptions bqOptions) {
    return jobService;
  }

  @Override
  public DatasetService getDatasetService(BigQueryOptions bqOptions) {
    return datasetService;
  }

  @Override
  public BigQueryJsonReader getReaderFromTable(
      BigQueryOptions bqOptions, TableReference tableRef) {
    return new FakeBigQueryReader(jsonTableRowReturns);
  }

  @Override
  public BigQueryJsonReader getReaderFromQuery(
      BigQueryOptions bqOptions, String projectId, JobConfigurationQuery queryConfig) {
    return new FakeBigQueryReader(jsonTableRowReturns);
  }

  private static class FakeBigQueryReader implements BigQueryJsonReader {
    private static final int UNSTARTED = -1;
    private static final int CLOSED = Integer.MAX_VALUE;

    private String[] jsonTableRowReturns;
    private int currIndex;

    FakeBigQueryReader(String[] jsonTableRowReturns) {
      this.jsonTableRowReturns = jsonTableRowReturns;
      this.currIndex = UNSTARTED;
    }

    @Override
    public boolean start() throws IOException {
      assertEquals(UNSTARTED, currIndex);
      currIndex = 0;
      return currIndex < jsonTableRowReturns.length;
    }

    @Override
    public boolean advance() throws IOException {
      return ++currIndex < jsonTableRowReturns.length;
    }

    @Override
    public TableRow getCurrent() throws NoSuchElementException {
      if (currIndex >= jsonTableRowReturns.length) {
        throw new NoSuchElementException();
      }
      return fromJsonString(jsonTableRowReturns[currIndex], TableRow.class);
    }

    @Override
    public void close() throws IOException {
      currIndex = CLOSED;
    }
  }
}
