/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

/**
 * Options for Shuffle workers. Most users should not need to adjust the settings in this section.
 */
public interface DataflowPipelineShuffleOptions {
  /**
   * Disk source image to use by shuffle VMs for jobs.
   * @see <a href="https://developers.google.com/compute/docs/images"
   * >Compute Engine Images</a>
   */
  @Description("Dataflow shuffle VM disk image.")
  String getShuffleDiskSourceImage();
  void setShuffleDiskSourceImage(String value);
  
  /**
   * Number of workers to use with the shuffle appliance, or 0 to use
   * the default number of workers.
   */
  @Description("Number of shuffle workers, when using remote execution")
  int getShuffleNumWorkers();
  void setShuffleNumWorkers(int value);

  /**
   * Remote shuffle worker disk size, in gigabytes, or 0 to use the
   * default size.
   */
  @Description("Remote shuffle worker disk size, in gigabytes, or 0 to use the default size.")
  int getShuffleDiskSizeGb();
  void setShuffleDiskSizeGb(int value);

  /**
   * GCE <a href="https://developers.google.com/compute/docs/zones"
   * >availability zone</a> for launching shuffle workers.
   *
   * <p> Default is up to the service.
   */
  @Description("GCE availability zone for launching shuffle workers. "
      + "Default is up to the service")
  String getShuffleZone();
  void setShuffleZone(String value);
}
