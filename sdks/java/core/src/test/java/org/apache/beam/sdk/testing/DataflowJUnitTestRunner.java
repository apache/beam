/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * A test runner which can invoke a series of method or class test targets configuring
 * the {@link TestPipeline} to run against the Dataflow service based upon command-line
 * parameters specified.
 *
 * <p>Supported target definitions as command line parameters are:
 * <ul>
 *   <li>Class: "ClassName"
 *   <li>Method: "ClassName#methodName"
 * </ul>
 * Multiple parameters can be specified in sequence, which will cause the test
 * runner to invoke the tests in the specified order.
 *
 * <p>All tests will be executed after which, if any test had failed, the runner
 * will exit with a non-zero status code.
 */
public class DataflowJUnitTestRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowJUnitTestRunner.class);

  /**
   * Options which control a Dataflow JUnit test runner to invoke
   * a series of method and/or class targets.
   */
  @Description("Options which control a Dataflow JUnit test runner to invoke "
      + "a series of method and/or class targets.")
  public interface Options extends PipelineOptions {
    @Description("A list of supported test targets. Supported targets are 'ClassName' "
        + "or 'ClassName#MethodName'")
    @Validation.Required
    String[] getTest();
    void setTest(String[] values);
  }

  public static void main(String ... args) throws Exception {
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Set<ClassPath.ClassInfo> classes =
        ClassPath.from(ClassLoader.getSystemClassLoader()).getAllClasses();

    // Build a list of requested test targets
    List<Request> requests = new ArrayList<>();
    for (String testTarget : options.getTest()) {
      if (testTarget.contains("#")) {
        String[] parts = testTarget.split("#", 2);
        Class<?> klass = findClass(parts[0], classes);
        requests.add(Request.method(klass, parts[1]));
      } else {
        requests.add(Request.aClass(findClass(testTarget, classes)));
      }
    }

    // Set system properties required by TestPipeline so that it is able to execute tests
    // on the service.
    String beamTestPipelineOptions = new ObjectMapper().writeValueAsString(args);
    System.setProperty("beamTestPipelineOptions", beamTestPipelineOptions);

    // Run the set of tests
    boolean success = true;
    JUnitCore core = new JUnitCore();
    for (Request request : requests) {
      Result result = core.run(request);
      if (!result.wasSuccessful()) {
        for (Failure failure : result.getFailures()) {
          LOG.error(failure.getTestHeader(), failure.getException());
        }
        success = false;
      }
    }
    if (!success) {
      throw new IllegalStateException("Tests failed, check output logs for details.");
    }
  }

  private static final Class<?> findClass(
      final String simpleName, Set<ClassPath.ClassInfo> classes)
      throws ClassNotFoundException {
    Iterable<ClassPath.ClassInfo> matches =
        Iterables.filter(classes, new Predicate<ClassPath.ClassInfo>() {
      @Override
      public boolean apply(@Nullable ClassInfo input) {
        return input != null && simpleName.equals(input.getSimpleName());
      }
    });
    return Class.forName(Iterables.getOnlyElement(matches).getName());
  }
}
