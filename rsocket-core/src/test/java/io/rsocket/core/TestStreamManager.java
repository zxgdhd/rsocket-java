package io.rsocket.core;

import org.assertj.core.api.Assertions;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

final class TestStreamManager extends AbstractStreamManager {

  final Throwable error;

  TestStreamManager(@Nullable Throwable error, StreamIdSupplier streamIdSupplier) {
    super(streamIdSupplier);
    this.error = error;
  }

  @Override
  public synchronized int getNextId() {
    int nextStreamId = super.getNextId();

    if (error != null) {
      throw Exceptions.propagate(error);
    }

    return nextStreamId;
  }

  @Override
  public synchronized int addAndGetNextId(FrameHandler frameHandler) {
    int nextStreamId = super.addAndGetNextId(frameHandler);

    if (error != null) {
      super.remove(nextStreamId, frameHandler);
      throw Exceptions.propagate(error);
    }

    return nextStreamId;
  }

  public static TestStreamManager client(@Nullable Throwable e) {
    return new TestStreamManager(e, StreamIdSupplier.clientSupplier());
  }

  public static TestStreamManager client() {
    return client(null);
  }

  public TestStreamManager assertNoActiveStreams() {
    Assertions.assertThat(activeStreams).isEmpty();
    return this;
  }

  public TestStreamManager assertHasStream(int i, FrameHandler stream) {
    Assertions.assertThat(activeStreams).containsEntry(i, stream);
    return this;
  }
}
