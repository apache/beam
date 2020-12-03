package org.apache.beam.validate.runner;

import net.sf.json.JSONArray;
import org.apache.beam.validate.runner.service.BatchTestService;
import org.apache.beam.validate.runner.service.StreamTestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.FileWriter;
import java.io.IOException;

public class Main {
    public static void main(String args[]) {
        try {
            final Logger logger = LoggerFactory.getLogger(Main.class);

            JSONArray outputDetails = new JSONArray();

            logger.info("Processing Batch Jobs:");
            BatchTestService batchTestService = new BatchTestService();
            outputDetails.add(batchTestService.getBatchTests());

            logger.info("Processing Stream Jobs:");
            StreamTestService streamTestService = new StreamTestService();
            outputDetails.add(streamTestService.getStreamTests());

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
