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
package org.apache.beam.sdk.io.fileschematransform;

import java.io.Serializable;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/** An {@link XmlAdapter} for {@link DateTime}s. */
class XmlDateTimeAdapter extends XmlAdapter<String, DateTime> implements Serializable {
  private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTime();

  /** Converts a String into {@link DateTime} based on {@link ISODateTimeFormat}. */
  @Override
  public DateTime unmarshal(String v) throws Exception {
    return DateTime.parse(v, FORMATTER);
  }

  /** Converts a {@link DateTime} into String based on {@link ISODateTimeFormat}. */
  @Override
  public String marshal(DateTime v) throws Exception {
    return v.toString(FORMATTER);
  }
}
