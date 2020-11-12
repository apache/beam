package org.apache.beam.validate.runner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import model.Configuration;

import java.io.InputStream;

public class FileReaderUtil {

    private static final String FILE_PATH = "/configuration.yaml";

    public static Configuration readConfiguration() {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            InputStream inputStream = FileReaderUtil.class.getResourceAsStream(FILE_PATH);
            return mapper.readValue(inputStream,Configuration.class);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
