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
package org.apache.beam.sdk.testing;

import org.joda.time.DateTimeUtils;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * This {@link TestRule} resets the date time provider in Joda to the system date time provider
 * after tests.
 */
public class ResetDateTimeProvider extends ExternalResource {
  public void setDateTimeFixed(String iso8601) {
    setDateTimeFixed(ISODateTimeFormat.dateTime().parseMillis(iso8601));
  }

  public void setDateTimeFixed(long millis) {
    DateTimeUtils.setCurrentMillisFixed(millis);
  }

  @Override
  protected void after() {
    DateTimeUtils.setCurrentMillisSystem();
  }
}
