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
 *
 *  EDIT BY: NopAngel | Angel Nieto (FORK)
 *
 */

package org.apache.beam.sdk.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Lineage;

/**
 * File system interface in Beam for writing file systems agnostic code.
 */
public abstract class FileSystem<ResourceIdT extends ResourceId> {

    /**
     * Converts user-provided specs to {@link ResourceIdT ResourceIds}.
     * Resolves ambiguities in the user-provided specs.
     */
    protected abstract List<MatchResult> match(List<String> specs) throws IOException;

    /**
     * Returns a write channel for the specified resource.
     */
    protected abstract WritableByteChannel create(ResourceIdT resourceId, CreateOptions createOptions)
            throws IOException;

    /**
     * Returns a read channel for the specified resource.
     */
    protected abstract ReadableByteChannel open(ResourceIdT resourceId) throws IOException;

    /**
     * Copies a list of resources from source to destination.
     */
    protected abstract void copy(List<ResourceIdT> srcResourceIds, List<ResourceIdT> destResourceIds)
            throws IOException;

    /**
     * Renames resources with optional move options.
     */
    protected abstract void rename(
            List<ResourceIdT> srcResourceIds,
            List<ResourceIdT> destResourceIds,
            MoveOptions... moveOptions)
            throws IOException;

    /**
     * Deletes specified resources.
     */
    protected abstract void delete(Collection<ResourceIdT> resourceIds) throws IOException;

    /**
     * Creates a new {@link ResourceId} based on the given resource spec and directory flag.
     */
    protected abstract ResourceIdT matchNewResource(String singleResourceSpec, boolean isDirectory);

    /**
     * Returns the URI scheme defining the namespace of the FileSystem.
     */
    protected abstract String getScheme();

    public enum LineageLevel {
        FILE,
        TOP_LEVEL
    }

    /**
     * Reports {@link Lineage} metrics for the specified resource id at file level.
     */
    protected void reportLineage(ResourceIdT resourceId, Lineage lineage) {
        reportLineage(resourceId, lineage, LineageLevel.FILE);
    }

    /**
     * Reports {@link Lineage} metrics for the specified resource id at the given level.
     * Default implementation is a no-op.
     */
    protected void reportLineage(ResourceIdT unusedId, Lineage unusedLineage, LineageLevel level) {}
}

