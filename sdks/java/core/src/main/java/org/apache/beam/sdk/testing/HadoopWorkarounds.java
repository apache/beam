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

import static java.util.Arrays.asList;

import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * A simple class ensure winutils.exe can be found in the JVM.
 * <p>
 * See http://wiki.apache.org/hadoop/WindowsProblems for details.
 * <p>
 * Note: don't forget to add org.bouncycastle:bcpg-jdk16 dependency to use it.
 */
public class HadoopWorkarounds {
    /**
     * In practise this method only needs to be called once by JVM
     * since hadoop uses static variables to store it.
     * <p>
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
        // note this commented code requires commons-compress dependency (to add if we use that)

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
        final File winutils = new File(hadoopHome, "bin/winutils.exe");

        for (final String suffix : asList("", ".asc")) {
            final URL url;
            try {
                // this code is not a random URL - read HADOOP-10051
                // it is provided and signed with an ASF gpg key.

                // note: 2.6.3 cause 2.6.4, 2.7.1 don't have .asc
                url = new URL("https://github.com/steveloughran/winutils/"
                        + "raw/master/hadoop-2.6.3/bin/winutils.exe" + suffix);
            } catch (final MalformedURLException e) { // unlikely
                throw new IllegalArgumentException(e);
            }

            // download winutils.exe
            try {
                try (final InputStream is = url.openStream();
                     final OutputStream os = new FileOutputStream(
                             new File(hadoopHome, "bin/winutils.exe" + suffix))) {
                    try {
                        IOUtils.copy(is, os, 1024 * 1024);
                    } catch (final IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        // get the gpg key which is supposed to have signed the winutils.exe
        final File gpg = new File(hadoopHome, "bin/gpg");
        try {
            /*
            key is https://github.com/steveloughran/winutils/blob/master/KEYS
            bu we trust the ASF not github so use the one we trust.
             */
            final URL gpgUrl = new URL("http://home.apache.org/keys/committer/stevel");
            try (final InputStream is = gpgUrl.openStream();
                 final OutputStream os = new FileOutputStream(gpg)) {
                try {
                    IOUtils.copy(is, os);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        final File ascFile = new File(winutils.getParentFile(), winutils.getName() + ".asc");
        try {
            sanityCheck(winutils, ascFile, gpg);
        } catch (IOException e) {
            throw new IllegalStateException("Invalid download");
        }

        System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath());
    }

    // TODO: replace with gpg --verify?
    // for now it is just some basic sanity checks to ensure we use the files we think
    private static void sanityCheck(
            final File winutils, final File ascFile, final File gpg)
            throws IOException {

        final byte[] asc = Files.readAllBytes(ascFile.toPath());
        final byte[] expectedAsc = ("-----BEGIN PGP SIGNATURE-----\n"
                + "Comment: GPGTools - https://gpgtools.org\n"
                + "\n"
                + "iQIcBAABCgAGBQJWeb5GAAoJEKkkVPkXR4a0qUgP/1u1Z5vV+IvU/8w79HIYX56+\n"
                + "FHMRGxM5953dggqjhGSBtfx62YA8oxhDP+8qLpQWtfjTC3//CW1Oz5hrkL0m+Am5\n"
                + "Kf+qiINDLqX3Fsc4wHQvnLMt2pJPmm4K9FtpkedCdAchLOiM6Wr7WtGiWYQAdUh0\n"
                + "5FjUZLLVx95Kj3cTY+1B/BL+z/hB63Ry2AC29oZG4fCuAH1nTZjhH3vBD1/kzS+E\n"
                + "LEKHrGh/pP6ADgg9AfJvVmRhidlCVi21ZfwWHAaitwDTMFvtFSGq03A3F6Xn2iyQ\n"
                + "3H6RcZ8dqEbtUEa1jOh1xNGzqP4oipWe0KQJ/Lx2eiSh8te73k/Pfw1Ta9CuHXqk\n"
                + "n8ko7cBc/pUm7nXbfjiURtWFJ4corT4oahJQna+GgvYR4BrYVLlSGb5VijTkzb7i\n"
                + "0XU40BM5sOcDS/I0lkvqKP0mSi+mMJXbm10y0jw2S7KR7KeHLwzybsjco05DfWUD\n"
                + "fSaCHK726g5SLsWJvZaurwna7+Mepzmo1HpAVy6nAuiAa2OQVIioNyFanIbuhbM3\n"
                + "7PXBDWbfPOgr1WbYW4TASoepvsuJsAahYf2SlGagByOiDNliDHJi1z+ArfWsCFFh\n"
                + "fAMMzPLKJwkmKPahyej3MrcywtntX68D7R8wTCAaj3xCxJsvX4IRv6YRk1+hQ2je\n"
                + "EXQFW2c8nTI6XqtFpsbw\n"
                + "=42+k\n"
                + "-----END PGP SIGNATURE-----\n").getBytes("UTF-8");
        if (!Arrays.equals(asc, expectedAsc)) {
            throw new IllegalArgumentException(
                    "Invalid asc file, did the repo get corrupted?");
        }

        final byte[] exe = Files.readAllBytes(winutils.toPath());
        if (exe.length != 108032 || exe[0] != 77
                || exe[exe.length - 1] != 0 || exe[exe.length / 3] != -127) {
            throw new IllegalArgumentException(
                    "Invalid winutils.exe file, did the repo get corrupted?");
        }

        // for now we ignore gpg cause it is useless until we can use gpg tools
    }

    /**
     * Just a convenient win(File) invocation for tests.
     */
    public static void winTests() {
        win(new File("target/hadoop-win"));
    }
}
