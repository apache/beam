package org.apache.beam.sdk.io.gcp.bigquery;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This used-case handles situation where bigquery project id may differ from default project.
 * Used to mark unit test method where the project id will be overridden with supplied project id through pipeline options.
 * The annotation is checked in @Rule.
 **/
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface ProjectOverride {
}
