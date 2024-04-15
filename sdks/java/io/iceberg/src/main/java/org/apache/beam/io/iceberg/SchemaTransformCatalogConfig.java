package org.apache.beam.io.iceberg;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.iceberg.CatalogUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SchemaTransformCatalogConfig {
    public static Builder builder() {
        return new AutoValue_SchemaTransformCatalogConfig.Builder();
    }

    public abstract String getCatalogName();

    public abstract @Nullable String getCatalogType();

    public abstract @Nullable String getCatalogImplementation();

    public abstract @Nullable String getWarehouseLocation();

    @AutoValue.Builder
    public abstract static class Builder {

        public abstract Builder setCatalogName(String catalogName);

        public abstract Builder setCatalogType(String catalogType);

        public abstract Builder setCatalogImplementation(String catalogImplementation);

        public abstract Builder setWarehouseLocation(String warehouseLocation);

        public abstract SchemaTransformCatalogConfig build();
    }

    Set<String> validTypes =
            Sets.newHashSet(
                    CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
                    CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                    CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    public void validate() {
        if (Strings.isNullOrEmpty(getCatalogType())) {
            checkArgument(
                    validTypes.contains(Preconditions.checkArgumentNotNull(getCatalogType())),
                    "Invalid catalog type. Please pick one of %s",
                    validTypes);
        }
    }
}
