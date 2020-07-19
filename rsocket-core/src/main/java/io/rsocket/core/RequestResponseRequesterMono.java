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

import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.core.PayloadValidationUtils.isValid;
import static io.rsocket.core.ReassemblyUtils.handleNextSupport;
import static io.rsocket.core.SendUtils.sendReleasingPayload;
import static io.rsocket.core.StateUtils.addRequestN;
import static io.rsocket.core.StateUtils.isFirstFrameSent;
import static io.rsocket.core.StateUtils.isReassembling;
import static io.rsocket.core.StateUtils.isSubscribed;
import static io.rsocket.core.StateUtils.isTerminated;
import static io.rsocket.core.StateUtils.isWorkInProgress;
import static io.rsocket.core.StateUtils.lazyTerminate;
import static io.rsocket.core.StateUtils.markFirstFrameSent;
import static io.rsocket.core.StateUtils.markSubscribed;
import static io.rsocket.core.StateUtils.markTerminated;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class RequestResponseRequesterMono extends Mono<Payload>
    implements RequesterFrameHandler, ReassembledFramesHolder, Subscription, Scannable {

  final ByteBufAllocator allocator;
  final Payload payload;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final StreamManager streamManager;
  final UnboundedProcessor<ByteBuf> sendProcessor;
  final PayloadDecoder payloadDecoder;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestResponseRequesterMono> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestResponseRequesterMono.class, "state");

  int streamId;
  CoreSubscriber<? super Payload> actual;
  CompositeByteBuf frames;
  boolean done;

  RequestResponseRequesterMono(
      ByteBufAllocator allocator,
      Payload payload,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize,
      StreamManager streamManager,
      UnboundedProcessor<ByteBuf> sendProcessor,
      PayloadDecoder payloadDecoder) {

    this.allocator = allocator;
    this.payload = payload;
    this.mtu = mtu;
    this.maxFrameLength = maxFrameLength;
    this.maxInboundPayloadSize = maxInboundPayloadSize;
    this.streamManager = streamManager;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {

    long previousState = markSubscribed(STATE, this);
    if (isSubscribed(previousState) || isTerminated(previousState)) {
      Operators.error(
          actual, new IllegalStateException("RequestResponseMono allows only a single Subscriber"));
      return;
    }

    final Payload p = this.payload;
    try {
      if (!isValid(this.mtu, this.maxFrameLength, p, false)) {
        lazyTerminate(STATE, this);
        Operators.error(
            actual,
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        p.release();
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      Operators.error(actual, e);
      return;
    }

    this.actual = actual;
    actual.onSubscribe(this);
  }

  @Override
  public final void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    long previousState = addRequestN(STATE, this, n);
    if (isTerminated(previousState) || isWorkInProgress(previousState)) {
      return;
    }

    sendFirstPayload(this.payload, n);
  }

  void sendFirstPayload(Payload payload, long initialRequestN) {

    final StreamManager sm = this.streamManager;
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;

    final int streamId;
    try {
      streamId = sm.addAndGetNextId(this);
      this.streamId = streamId;
    } catch (Throwable t) {
      this.done = true;
      lazyTerminate(STATE, this);
      payload.release();
      this.actual.onError(Exceptions.unwrap(t));
      return;
    }

    try {
      sendReleasingPayload(
          streamId, FrameType.REQUEST_RESPONSE, this.mtu, payload, sender, allocator, true);
    } catch (Throwable e) {
      this.done = true;
      lazyTerminate(STATE, this);

      sm.remove(streamId, this);

      this.actual.onError(e);
      return;
    }

    long previousState = markFirstFrameSent(STATE, this);
    if (isTerminated(previousState)) {
      if (this.done) {
        return;
      }

      sm.remove(streamId, this);

      final ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, streamId);
      sender.onNext(cancelFrame);
    }
  }

  @Override
  public final void cancel() {
    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    if (isFirstFrameSent(previousState)) {
      final int streamId = this.streamId;
      this.streamManager.remove(streamId, this);

      final CompositeByteBuf frames = this.frames;
      this.frames = null;
      if (isReassembling(previousState)) {
        synchronized (frames) {
          frames.release();
        }
      }

      this.sendProcessor.onNext(CancelFrameCodec.encode(this.allocator, streamId));
    } else if (!isWorkInProgress(previousState)) {
      this.payload.release();
    }
  }

  @Override
  public final void handlePayload(Payload value) {
    if (this.done) {
      value.release();
      return;
    }

    this.done = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      value.release();
      return;
    }

    final CoreSubscriber<? super Payload> a = this.actual;

    this.streamManager.remove(this.streamId, this);

    a.onNext(value);
    a.onComplete();
  }

  @Override
  public final void handleComplete() {
    if (this.done) {
      return;
    }

    this.done = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    this.streamManager.remove(this.streamId, this);

    this.actual.onComplete();
  }

  @Override
  public final void handleError(Throwable cause) {
    if (this.done) {
      Operators.onErrorDropped(cause, this.actual.currentContext());
      return;
    }

    this.done = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.actual.currentContext());
      return;
    }

    final CompositeByteBuf frames = this.frames;
    this.frames = null;
    if (isReassembling(previousState)) {
      synchronized (frames) {
        frames.release();
      }
    }

    this.streamManager.remove(this.streamId, this);

    this.actual.onError(cause);
  }

  @Override
  public void handleNext(ByteBuf frame, boolean hasFollows, boolean isLastPayload) {
    handleNextSupport(
        STATE,
        this,
        this,
        this,
        this.actual,
        this.payloadDecoder,
        this.allocator,
        this.maxInboundPayloadSize,
        frame,
        hasFollows,
        isLastPayload);
  }

  @Override
  public CompositeByteBuf getFrames() {
    return this.frames;
  }

  @Override
  public void setFrames(CompositeByteBuf byteBuf) {
    this.frames = byteBuf;
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    long state = this.state;

    if (key == Attr.TERMINATED) return isTerminated(state);
    if (key == Attr.PREFETCH) return 0;

    return null;
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(RequestResponseMono)";
  }
}
