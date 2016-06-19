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

import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * A simple class ensure winutils.exe can be found in the JVM.
 */
public class HadoopWorkarounds {
    /**
     * In practise this method only needs to be called once by JVM
     * since hadoop uses static variables to store it.
     *
     * Note: ensure invocation is done before hadoop reads it
     * and ensure this folder survives tests
     * (avoid temporary folder usage since tests can share it).
     *
     * @param hadoopHome where to fake hadoop home.
     */
    public static void win(final File hadoopHome) {
        // if (Shell.osType != Shell.OSType.OS_TYPE_WIN) { // don't do that to not load Shell yet
        if (!System.getProperty("os.name", "").startsWith("Windows")
                || System.getProperty("hadoop.home.dir") != null) {
            return;
        }

        // hadoop doesn't have winutils.exe :(: https://issues.apache.org/jira/browse/HADOOP-10051
        // so use this github repo temporarly then just use the main tar.gz
        /*
        String hadoopVersion = VersionInfo.getVersion();
        final URL url = new URL("https://archive.apache.org/dist/hadoop/common/
                  hadoop-" + hadoopVersion + "/hadoop-" + hadoopVersion + ".tar.gz");
        final File hadoopTar = tmpFolder.newFile();
        try (final InputStream is = new GZIPInputStream(url.openStream());
             final OutputStream os = new FileOutputStream(hadoopTar)) {
          System.out.println("Downloading Hadoop in " + hadoopTar + ", " +
                  "this can take a while, if you have it locally " +
                  "maybe set \"hadoop.home.dir\" system property");
          IOUtils.copyLarge(is, os, new byte[1024 * 1024]);
        }

        final File hadoopHome = tmpFolder.newFolder();
        try (final ArchiveInputStream stream = new TarArchiveInputStream(
                new FileInputStream(hadoopTar))) {
          ArchiveEntry entry;
          while ((entry = stream.getNextEntry()) != null) {
            if (entry.isDirectory()) {
              FileUtils.forceMkdir(new File(hadoopHome, entry.getName()));
              continue;
            }
            final File out = new File(hadoopHome, entry.getName());
            FileUtils.forceMkdir(out.getParentFile());
            try (final OutputStream os = new FileOutputStream(out)) {
              IOUtils.copy(stream, os);
            }
          }
        }

        final String hadoopRoot = "hadoop-" + hadoopVersion;
        final File[] files = hadoopHome.listFiles(new FileFilter() {
          @Override
          public boolean accept(final File pathname) {
            return pathname.isDirectory() && pathname.getName().equals(hadoopRoot);
          }
        });
        if (files == null || files.length != 1) {
          throw new IllegalStateException("Didn't find hadoop in " + hadoopHome);
        }
        System.setProperty("hadoop.home.dir", files[0].getAbsolutePath());
        */

        System.out.println("You are on windows (sorry) and you don't set "
                + "-Dhadoop.home.dir so we'll download winutils.exe");

        new File(hadoopHome, "bin").mkdirs();
        final URL url;
        try {
            url = new URL("https://github.com/steveloughran/winutils/"
                    + "raw/master/hadoop-2.7.1/bin/winutils.exe");
        } catch (final MalformedURLException e) { // unlikely
            throw new IllegalArgumentException(e);
        }
        try {
            try (final InputStream is = url.openStream();
                 final OutputStream os = new FileOutputStream(
                         new File(hadoopHome, "bin/winutils.exe"))) {
                try {
                    IOUtils.copy(is, os, 1024 * 1024);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath());
    }

    /**
     * Just a convenient win(File) invocation for tests.
     */
    public static void winTests() {
        win(new File("target/hadoop-win"));
    }
}
