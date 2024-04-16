/*
 * Copyright 2023 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce.io.solace.read;

import com.google.cloud.dataflow.dce.io.solace.broker.MessageReceiver;
import com.google.cloud.dataflow.dce.io.solace.broker.SempClient;
import com.google.cloud.dataflow.dce.io.solace.broker.SessionService;
import com.google.common.annotations.VisibleForTesting;
import com.solacesystems.jcsmp.BytesXMLMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unbounded Reader to read messages from a Solace Router. */
@VisibleForTesting
class UnboundedSolaceReader<T> extends UnboundedSource.UnboundedReader<T> {

    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSolaceReader.class);
    private final UnboundedSolaceSource<T> currentSource;
    private final WatermarkPolicy<T> watermarkPolicy;
    AtomicBoolean active = new AtomicBoolean(true);
    private BytesXMLMessage solaceOriginalRecord;
    private T solaceMappedRecord;
    private MessageReceiver messageReceiver;
    private SessionService sessionService;
    private final SempClient sempClient;

    /**
     * Queue to place advanced messages before {@link #getCheckpointMark()} be called non-concurrent
     * queue, should only be accessed by the reader thread A given {@link UnboundedReader} object
     * will only be accessed by a single thread at once.
     */
    private final java.util.Queue<BytesXMLMessage> elementsToCheckpoint = new ArrayDeque<>();

    public UnboundedSolaceReader(UnboundedSolaceSource<T> currentSource) {
        this.currentSource = currentSource;
        this.watermarkPolicy = WatermarkPolicy.create(currentSource.getTimestampFn());
        this.sessionService = currentSource.getSessionServiceFactory().create();
        this.sempClient = currentSource.getSempClientFactory().create();
    }

    @Override
    public boolean start() {
        populateSession();
        populateMessageConsumer();
        return advance();
    }

    public void populateSession() {
        if (sessionService == null) {
            sessionService = getCurrentSource().getSessionServiceFactory().create();
        }
        if (sessionService.isClosed()) {
            sessionService.connect();
        }
    }

    private void populateMessageConsumer() {
        if (messageReceiver == null) {
            messageReceiver = sessionService.createReceiver();
            messageReceiver.start();
        }
        if (messageReceiver.isClosed()) {
            messageReceiver.start();
        }
    }

    @Override
    public boolean advance() {
        BytesXMLMessage receivedXmlMessage;
        try {
            receivedXmlMessage = messageReceiver.receive();
        } catch (IOException e) {
            LOG.warn("SolaceIO.Read: Exception when pulling messages from the broker.", e);
            return false;
        }

        if (receivedXmlMessage == null) {
            return false;
        }
        elementsToCheckpoint.add(receivedXmlMessage);
        solaceOriginalRecord = receivedXmlMessage;
        solaceMappedRecord = getCurrentSource().getParseFn().apply(receivedXmlMessage);
        watermarkPolicy.update(solaceMappedRecord);
        return true;
    }

    @Override
    public void close() {
        active.set(false);
        sessionService.close();
    }

    @Override
    public Instant getWatermark() {
        // should be only used by a test receiver
        if (messageReceiver.isEOF()) {
            return BoundedWindow.TIMESTAMP_MAX_VALUE;
        }
        return watermarkPolicy.getWatermark();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        List<BytesXMLMessage> ackQueue = new ArrayList<>();
        while (!elementsToCheckpoint.isEmpty()) {
            BytesXMLMessage msg = elementsToCheckpoint.poll();
            ackQueue.add(msg);
        }
        return new SolaceCheckpointMark(active, ackQueue);
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
        if (solaceMappedRecord == null) {
            throw new NoSuchElementException();
        }
        return solaceMappedRecord;
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
        if (solaceOriginalRecord == null) {
            throw new NoSuchElementException();
        }
        if (solaceOriginalRecord.getApplicationMessageId() != null) {
            return solaceOriginalRecord.getApplicationMessageId().getBytes(StandardCharsets.UTF_8);
        } else {
            return solaceOriginalRecord
                    .getReplicationGroupMessageId()
                    .toString()
                    .getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    public UnboundedSolaceSource<T> getCurrentSource() {
        return currentSource;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (getCurrent() == null) {
            throw new NoSuchElementException();
        }
        return currentSource.getTimestampFn().apply(getCurrent());
    }

    @Override
    public long getTotalBacklogBytes() {
        try {
            return sempClient.getBacklogBytes(currentSource.getQueue().getName());
        } catch (IOException e) {
            LOG.warn("SolaceIO.Read: Could not query backlog bytes. Returning BACKLOG_UNKNOWN", e);
            return BACKLOG_UNKNOWN;
        }
    }
}
