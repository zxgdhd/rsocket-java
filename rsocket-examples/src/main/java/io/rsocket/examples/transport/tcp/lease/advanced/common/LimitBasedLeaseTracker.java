package io.rsocket.examples.transport.tcp.lease.advanced.common;

import com.netflix.concurrency.limits.Limit;
import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import io.rsocket.lease.LeaseTracker;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

public class LimitBasedLeaseTracker implements LeaseTracker {

  final String connectionId;
  final PeriodicLeaseSender parent;
  final Limit limitAlgorithm;

  final ConcurrentMap<Integer, Integer> inFlightMap = new ConcurrentHashMap<>();
  final ConcurrentMap<Integer, Long> timeMap = new ConcurrentHashMap<>();

  final LongSupplier clock = System::nanoTime;

  public LimitBasedLeaseTracker(
      String connectionId, PeriodicLeaseSender parent, Limit limitAlgorithm) {
    this.connectionId = connectionId;
    this.parent = parent;
    this.limitAlgorithm = limitAlgorithm;
  }

  @Override
  public void onAccept(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
    long startTime = clock.getAsLong();

    int currentInFlight = parent.incrementInFlightAndGet();

    inFlightMap.put(streamId, currentInFlight);
    timeMap.put(streamId, startTime);
  }

  @Override
  public void onReject(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {}

  @Override
  public void onRelease(int streamId, SignalType terminalSignal) {
    parent.decrementInFlight();

    Long startTime = timeMap.remove(streamId);
    Integer currentInflight = inFlightMap.remove(streamId);

    limitAlgorithm.onSample(
        startTime,
        clock.getAsLong() - startTime,
        currentInflight,
        !(terminalSignal == SignalType.ON_COMPLETE));
  }

  @Override
  public void onClose() {
    parent.remove(this);
  }
}
