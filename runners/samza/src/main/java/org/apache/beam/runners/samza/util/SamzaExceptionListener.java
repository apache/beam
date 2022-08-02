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
package org.apache.beam.runners.samza.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Map;

/**
 * An ExceptionListener following Observer pattern. Any class implementing {@code
 * PropertyChangeListener} can be registered with SamzaExceptionListener. Any runtime exception
 * caught by {@code OpAdapter} will be notified to all registered PropertyChangeListener(s) as a
 * {@code PropertyChangeEvent}
 */
public class SamzaExceptionListener {
  private PropertyChangeSupport support;
  private static final SamzaExceptionListener singletonExceptionListener =
      new SamzaExceptionListener();

  private SamzaExceptionListener() {
    support = new PropertyChangeSupport(this);
  }

  public static SamzaExceptionListener getInstance() {
    return singletonExceptionListener;
  }

  public void addPropertyChangeListener(PropertyChangeListener pcl) {
    support.addPropertyChangeListener(pcl);
  }

  public void removePropertyChangeListener(PropertyChangeListener pcl) {
    support.removePropertyChangeListener(pcl);
  }

  public void setException(Map.Entry<String, Exception> value) {
    support.firePropertyChange("exception", null, value);
  }
}
