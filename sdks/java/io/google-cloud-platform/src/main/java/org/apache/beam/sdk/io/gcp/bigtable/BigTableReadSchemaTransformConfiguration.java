package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.sdk.io.FileSystems;

/**
 * Configuration for reading from BigTable
 *
 * This class is used with {@link BigtableReadSchemaTransformProvider}
 *
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigTableReadSchemaTransformConfiguration implements Serializable{

    public void validate() {
        String invalidConfigMessage = "Invalid TFRecord Read configuration: ";

        if (getValidate()) {
            String filePattern = getFilePattern();
            try {
                MatchResult matches = FileSystems.match(filePattern);
                checkState(
                        !matches.metadata().isEmpty(), "Unable to find any files matching %s", filePattern);
            } catch (IOException e) {
                throw new IllegalStateException(
                        String.format(invalidConfigMessage + "Failed to validate %s", filePattern), e);
            }
        }

        ErrorHandling errorHandling = getErrorHandling();
        if (errorHandling != null) {
            checkArgument(
                    !Strings.isNullOrEmpty(errorHandling.getOutput()),
                    invalidConfigMessage + "Output must not be empty if error handling specified.");
        }
    }

    /** Instantiates a {@link BigTableReadSchemaTransformConfiguration.Builder} instance. */
    public static BigTableReadSchemaTransformConfiguration.Builder builder() {
        return new AutoValue_BigTableReadSchemaTransformConfiguration.Builder();
    }

    @SchemaFieldDescription("Validate file pattern.")
    public abstract boolean getValidate();

    @SchemaFieldDescription("Decompression type to use when reading input files.")
    public abstract String getCompression();

    @SchemaFieldDescription("Filename or file pattern used to find input files.")
    public abstract String getFilePattern();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    public abstract @Nullable ErrorHandling getErrorHandling();

    abstract Builder toBuilder();

    /** Builder for {@link BigTableReadSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {

        public abstract Builder setValidate(boolean value);

        public abstract Builder setCompression(String value);

        public abstract Builder setFilePattern(String value);

        public abstract Builder setErrorHandling(@Nullable ErrorHandling errorHandling);

        /** Builds the {@link BigTableReadSchemaTransformConfiguration} configuration. */
        public abstract BigTableReadSchemaTransformConfiguration build();
    }
}
