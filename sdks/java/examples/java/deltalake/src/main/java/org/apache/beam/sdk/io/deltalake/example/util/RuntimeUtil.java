package org.apache.beam.sdk.io.deltalake.example.util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class RuntimeUtil
{
    private static final Logger logger = LoggerFactory.getLogger(RuntimeUtil.class);

    public static void runForever(Pipeline pipeline)
    {
        try {
            PipelineResult.State result = pipeline.run().waitUntilFinish();
            logger.info("Pipeline execution completed: {}", result.toString());
        } catch (Exception e) {
            logger.error("Pipeline execution error:", e);
        }

        long ONE_MINUTE_IN_MS = 60L * 1000L;
        long endTime = System.currentTimeMillis();
        Date d = new Date();

        while (true) {
            long dt = (System.currentTimeMillis() - endTime) / ONE_MINUTE_IN_MS;
            logger.info("Pipeline completed at {}, {} minutes ago", d, dt);
            try { Thread.sleep(ONE_MINUTE_IN_MS); } catch (InterruptedException e) { /* ignore exception */ }
        }
    }

    public static List<MatchResult.Metadata> getFiles(String filePattern, boolean logNames) throws IOException
    {
        // pattern:
        //    .../* top level files, without dir
        //    .../**  top level files & files on all directories

        String allFiles = (filePattern.endsWith("*.parquet")) ?
            filePattern.replace("*.parquet", "**") :
            filePattern;

        MatchResult match = FileSystems.match(allFiles, EmptyMatchTreatment.ALLOW);
        logger.info("getFiles found {} files for pattern {}", match.metadata().size(), allFiles);
        List<MatchResult.Metadata> matchMetadata = match.metadata();
        if (logNames) {
            for (MatchResult.Metadata meta : matchMetadata) {
                logger.info("  ::> {}, isDir={}",  meta, meta.resourceId().isDirectory());
            }
        }
        return matchMetadata;
    }


}
