package org.beam.sdk.java.sql.schema;

import java.io.Serializable;

/**
 * Type as a source IO, determined whether it's a STREAMING process, or batch
 * process.
 */
public enum BeamIOType implements Serializable {
  BOUNDED, UNBOUNDED;
}
