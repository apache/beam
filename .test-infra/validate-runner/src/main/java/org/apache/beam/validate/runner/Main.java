package org.apache.beam.validate.runner;

import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.BatchTestService;
import service.StreamTestService;

import java.io.FileWriter;
import java.io.IOException;

public class Main {
    public static void main(String args[]) {
        try {
            final Logger logger = LoggerFactory.getLogger(JenkinsApi.class);

            JSONObject outputDetails = new JSONObject();

            logger.info("Processing Batch Jobs:");
            BatchTestService batchTestService = new BatchTestService();
            outputDetails.put("batch", batchTestService.getBatchTests());

            logger.info("Processing Stream Jobs:");
            StreamTestService streamTestService = new StreamTestService();
            outputDetails.put("stream", streamTestService.getStreamTests());

            try (FileWriter file = new FileWriter("out.json")) {
                file.write(outputDetails.toString(3));
                file.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
