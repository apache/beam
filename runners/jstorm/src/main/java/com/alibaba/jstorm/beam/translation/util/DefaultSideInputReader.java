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
package com.alibaba.jstorm.beam.translation.util;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * No-op SideInputReader implementation.
 */
public class DefaultSideInputReader implements SideInputReader, Serializable {
    @Nullable
    @Override
    public <T> T get(PCollectionView<T> pCollectionView, BoundedWindow boundedWindow) {
        return null;
    }

    @Override
    public <T> boolean contains(PCollectionView<T> pCollectionView) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }
}
