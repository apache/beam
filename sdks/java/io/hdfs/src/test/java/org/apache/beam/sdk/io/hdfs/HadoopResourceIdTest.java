package org.apache.beam.sdk.io.hdfs;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for the HadoopResourceId class.
 */
public class HadoopResourceIdTest {
  @Test
  public void fromAndToPath() {
    // Directory path without slash
    Path dirPathWithoutSlash = new Path("hdfs://myhost/mydir");
    HadoopResourceId resourceDirWithoutSlash = HadoopResourceId.fromPath(dirPathWithoutSlash);
    assertEquals("hdfs://myhost/mydir",
        resourceDirWithoutSlash.toString());
    assertEquals(dirPathWithoutSlash, resourceDirWithoutSlash.getPath());

    // Directory path with slash
    Path dirPathWithSlash = new Path("hdfs://myhost/mydir/");
    HadoopResourceId resourceDirWithSlash = HadoopResourceId.fromPath(dirPathWithSlash);
    assertEquals("hdfs://myhost/mydir",
        resourceDirWithSlash.toString());
    assertEquals(dirPathWithSlash, resourceDirWithSlash.getPath());

    // File path
    Path filePath = new Path("hdfs://myhost/mydir/myfile.txt");
    HadoopResourceId resourceFile = HadoopResourceId.fromPath(filePath);
    assertEquals("hdfs://myhost/mydir/myfile.txt",
        resourceFile.toString());
    assertEquals(filePath, resourceFile.getPath());
  }

  @Test
  public void handlesRelativePathsAddedToDir() {
    // Directory + file - slash on Directory
    HadoopResourceId dirWithSlash = HadoopResourceId.fromPath(new Path("hdfs://myhost/mydir/"));
    assertEquals("hdfs://myhost/mydir/myfile.txt",
        dirWithSlash.resolve("myfile.txt",
            ResolveOptions.StandardResolveOptions.RESOLVE_FILE).toString());

    // Directory + Directory
    assertEquals("hdfs://myhost/mydir/2nddir",
        dirWithSlash.resolve("2nddir",
            ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY).toString());
    assertEquals("hdfs://myhost/mydir/2nddir",
        dirWithSlash.resolve("2nddir/",
            ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY).toString());


    // Directory + File - no slash on either
    HadoopResourceId dirWithoutSlash = HadoopResourceId.fromPath(new Path("hdfs://myhost/mydir"));
    assertEquals("hdfs://myhost/mydir/myfile.txt",
        dirWithoutSlash.resolve("myfile.txt",
            ResolveOptions.StandardResolveOptions.RESOLVE_FILE).toString());
  }

  @Test
  public void testScheme() {
    assertEquals("hdfs",
        HadoopResourceId.fromPath(new Path("hdfs://myhost/mydir/file.txt")).getScheme());
  }
}
