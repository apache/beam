package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A {@code OverridablePTransform} is an interface that provides a API for users to override
 * a {@link PTransform} when needed (e.g. local tests). When tag provided by a {@code
 * OverridablePTransform} is registered in {@link PTransformOverrideRegistrar}, the {@link
 * OverridablePTransform} will be replaced.
 *
 * LI-specific class.
 */
@Experimental
public interface OverridablePTransform<IN extends PInput, OUT extends POutput> {
    String getTag();
}

