/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toForkRequest;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import javax.annotation.Nullable;

/**
 * Helpers for testing {@code Reader} and related classes, especially
 * {@link Reader.ReaderIterator#getProgress} and {@link Reader.ReaderIterator#requestFork}.
 */
public class ReaderTestUtils {
  public static Position positionAtIndex(@Nullable Long index) {
    return new Position().setRecordIndex(index);
  }

  public static Position positionAtByteOffset(@Nullable Long byteOffset) {
    return new Position().setByteOffset(byteOffset);
  }

  public static ApproximateProgress approximateProgressAtPosition(@Nullable Position position) {
    return new ApproximateProgress().setPosition(position);
  }

  public static ApproximateProgress approximateProgressAtIndex(@Nullable Long index) {
    return approximateProgressAtPosition(positionAtIndex(index));
  }

  public static ApproximateProgress approximateProgressAtByteOffset(@Nullable Long byteOffset) {
    return approximateProgressAtPosition(positionAtByteOffset(byteOffset));
  }

  public static Reader.ForkRequest forkRequestAtPosition(@Nullable Position position) {
    return toForkRequest(approximateProgressAtPosition(position));
  }

  public static Reader.ForkRequest forkRequestAtIndex(@Nullable Long index) {
    return toForkRequest(approximateProgressAtIndex(index));
  }

  public static Reader.ForkRequest forkRequestAtByteOffset(@Nullable Long byteOffset) {
    return toForkRequest(approximateProgressAtByteOffset(byteOffset));
  }

  public static Position positionFromForkResult(Reader.ForkResult forkResult) {
    return toCloudPosition(((Reader.ForkResultWithPosition) forkResult).getAcceptedPosition());
  }

  public static Position positionFromProgress(Reader.Progress progress) {
    return readerProgressToCloudProgress(progress).getPosition();
  }
}
