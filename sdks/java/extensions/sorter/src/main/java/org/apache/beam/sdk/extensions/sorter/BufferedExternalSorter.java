package org.apache.beam.sdk.extensions.sorter;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.values.KV;

/**
 * Sorter that will use in memory sorting until the values can't fit into memory and will then fall
 * back to external sorting.
 */
public class BufferedExternalSorter implements Sorter {
  /** Contains configuration for the sorter. */
  public static class Options implements Serializable {
    private String tempLocation = "/tmp";
    private int memoryMB = 100;

    /** Sets the path to a temporary location where the sorter writes intermediate files. */
    public void setTempLocation(String tempLocation) {
      if (tempLocation.startsWith("gs://")) {
        throw new IllegalArgumentException("Sorter doesn't support GCS temporary location.");
      }

      this.tempLocation = tempLocation;
    }

    /** Returns the configured temporary location. */
    public String getTempLocation() {
      return tempLocation;
    }

    /**
     * Sets the size of the memory buffer in megabytes. This controls both the buffer for initial in
     * memory sorting and the buffer used when external sorting.
     */
    public void setMemoryMB(int memoryMB) {
      this.memoryMB = memoryMB;
    }

    /** Returns the configured size of the memory buffer. */
    public int getMemoryMB() {
      return memoryMB;
    }
  }

  private ExternalSorter externalSorter;
  private InMemorySorter inMemorySorter;

  boolean inMemorySorterFull;

  private BufferedExternalSorter(ExternalSorter externalSorter, InMemorySorter inMemorySorter) {
    this.externalSorter = externalSorter;
    this.inMemorySorter = inMemorySorter;
  }

  public static BufferedExternalSorter create(Options options) {
    ExternalSorter.Options externalSorterOptions = new ExternalSorter.Options();
    externalSorterOptions.setMemoryMB(options.getMemoryMB());
    externalSorterOptions.setTempLocation(options.getTempLocation());

    InMemorySorter.Options inMemorySorterOptions = new InMemorySorter.Options();
    inMemorySorterOptions.setMemoryMB(options.getMemoryMB());

    return new BufferedExternalSorter(
        ExternalSorter.create(externalSorterOptions), InMemorySorter.create(inMemorySorterOptions));
  }

  @Override
  public void add(KV<byte[], byte[]> record) throws IOException {
    if (!inMemorySorterFull) {
      if (inMemorySorter.addIfRoom(record)) {
        return;
      } else {
        // Flushing contents of in memory sorter to external sorter so we can rely on external
        // from here on out
        inMemorySorterFull = true;
        transferToExternalSorter();
      }
    }

    // In memory sorter is full, so put in external sorter instead
    externalSorter.add(record);
  }

  /**
   * Transfers all of the records loaded so far into the in memory sorter over to the external
   * sorter.
   */
  private void transferToExternalSorter() throws IOException {
    for (KV<byte[], byte[]> record : inMemorySorter.sort()) {
      externalSorter.add(record);
    }
    // Allow in memory sorter and its contents to be garbage collected
    inMemorySorter = null;
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    if (!inMemorySorterFull) {
      return inMemorySorter.sort();
    } else {
      return externalSorter.sort();
    }
  }
}
