package org.apache.beam.spd.command;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface StructuredPipelineRunOptions extends PipelineOptions {

    @Description("The location of your profile definitions.")
    @Nullable
    String getProfilePath();

    void setProfilePath();


    @Description("Specifies a target environment. Must be present in your profile.")
    @Nullable
    String getTarget();

    void setTarget(String target);
}
