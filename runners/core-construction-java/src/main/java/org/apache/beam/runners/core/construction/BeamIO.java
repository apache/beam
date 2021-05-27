package org.apache.beam.runners.core.construction;

import org.apache.beam.sdk.runners.TransformHierarchy;

/**
 * Interface to get I/O information for a Beam job.
 */
public interface BeamIO {

    /**
     * Get I/O topic name and cluster.
     */
    String getIOInfo(TransformHierarchy.Node node);

    /**
     * A registrar for {@link BeamIO}.
     */
    interface BeamIORegistrar {
        BeamIO getBeamIO();
    }
}
