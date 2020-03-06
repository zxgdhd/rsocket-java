/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.core;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

abstract class AbstractStreamManager implements StreamManager {

  final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<FrameHandler> activeStreams;

  protected AbstractStreamManager(StreamIdSupplier streamIdSupplier) {
    this.streamIdSupplier = streamIdSupplier;
    this.activeStreams = new IntObjectHashMap<>();
  }

  @Override
  public synchronized int getNextId() {
    return this.streamIdSupplier.nextStreamId(this.activeStreams);
  }

  @Override
  public synchronized int addAndGetNextId(FrameHandler frameHandler) {
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    final int streamId = this.streamIdSupplier.nextStreamId(activeStreams);

    activeStreams.put(streamId, frameHandler);

    return streamId;
  }

  @Override
  public synchronized FrameHandler get(int streamId) {
    return this.activeStreams.get(streamId);
  }

  @Override
  public synchronized boolean remove(int streamId, FrameHandler frameHandler) {
    return this.activeStreams.remove(streamId, frameHandler);
  }
}
