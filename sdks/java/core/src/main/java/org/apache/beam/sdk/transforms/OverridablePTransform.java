package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A {@code OverridablePTransform} is a {@link PTransform} that provides a API for users to override
 * a {@link PTransform} when needed (e.g. local tests). When urn provided by a {@code
 * OverridablePTransform} is registered in {@link PTransformOverrideRegistrar}, the {@link
 * OverridablePTransform} will be replaced.
 *
 * LI-specific class.
 */
@Experimental
public abstract class OverridablePTransform<IN extends PInput, OUT extends POutput>
        extends PTransform<IN, OUT> {

    public abstract String getTag();
}

