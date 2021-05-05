package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.commons.lang3.StringUtils;

/**
 * This is a singleton class that holds a map that mapping origin PTransform tag to replacement
 * PTransform. LI-specific class.
 */
@Experimental
public class PTransformOverrideRegistrar {

    private static final ThreadLocal<Map<String, PTransform>> PTRANSFORM_OVERRIDE_REGISTRAR_MAP =
        ThreadLocal.withInitial(HashMap::new);

    public static <InputT extends PInput, OutputT extends POutput>
    PTransform<? super InputT, OutputT> applyTransformOverride(
            InputT input, PTransform<? super InputT, OutputT> transform) {
      if (transform instanceof OverridablePTransform) {
        PTransform<? super InputT, OutputT> overriddenTransform =
            getOverriddenPTransform(((OverridablePTransform) transform).getTag());
        if (overriddenTransform != null) {
            return overriddenTransform;
        }
      }
      return transform;
    }

    public static void register(
        String originPTransformTag, PTransform replacementPTransform) {
      checkArgument(!StringUtils.isBlank(originPTransformTag), "PTramsform tag cannot be null.");
      checkArgument(
          !PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().containsKey(originPTransformTag),
          "PTransform tag: "
              + originPTransformTag
              + " is already registered with PTransform: "
              + PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().get(originPTransformTag));
      PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().put(originPTransformTag, replacementPTransform);
    }

    public static PTransform getOverriddenPTransform(String tag) {
      return PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().get(tag);
    }

    public static void clear() {
      PTRANSFORM_OVERRIDE_REGISTRAR_MAP.remove();
    }
}

