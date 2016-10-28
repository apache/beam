package org.apache.beam.sdk.util;

/**
 * Created by peihe on 10/31/16.
 */
public interface IOChannelFactoryConfigOptions {
  String getScheme();
  String getBaseDir();
  String getCredential();
}
