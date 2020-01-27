package org.apache.beam.runners.ignite;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options specific to the Ignite runner. */
public interface IgnitePipelineOptions extends PipelineOptions {

    @Description("Name of Ignite group")
    @Validation.Required
    @Default.String("ignite")
    String getIgniteGroupName();

    void setIgniteGroupName(String igniteGroupName);

    @Description("Specifies the addresses of the Ignite cluster; needed only with external clusters")
    @Validation.Required
    @Default.String("127.0.0.1:47500")
    String getIgniteServers();

    void setIgniteServers(String igniteServers);

    @Description(
            "Specifies where the fat-jar containing all the code is located; needed only with external clusters")
    String getCodeJarPathname();

    void setCodeJarPathname(String codeJarPathname);

    @Description("Local parallelism of Ignite nodes")
    @Validation.Required
    @Default.Integer(2)
    Integer getIgniteDefaultParallelism();

    void setIgniteDefaultParallelism(Integer localParallelism);

    @Description("Number of locally started Ignite Cluster Members")
    @Validation.Required
    @Default.Integer(0)
    Integer getIgniteLocalMode();

    void setIgniteLocalMode(Integer noOfLocalClusterMembers);

    @Description("Weather Ignite Processors for DoFns should use green threads or not")
    @Validation.Required
    @Default.Boolean(false)
    Boolean getIgniteProcessorsCooperative();

    void setIgniteProcessorsCooperative(Boolean cooperative);
}
