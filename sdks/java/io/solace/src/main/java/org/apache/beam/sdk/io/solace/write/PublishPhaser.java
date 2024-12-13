package org.apache.beam.sdk.io.solace.write;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;
import java.util.concurrent.Phaser;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;

public class PublishPhaser extends Phaser {
  public Queue<Map.Entry<String, PublishResult>> thrownExceptions = new ConcurrentLinkedQueue<>();
  public ConcurrentHashMap<String, PublishResult> successfulRecords = new ConcurrentHashMap<>();

  PublishPhaser(int parties) {
    super(parties);
  }
}