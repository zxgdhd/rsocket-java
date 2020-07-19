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
import static io.rsocket.core.StateUtils.extractRequestN;
import static io.rsocket.core.StateUtils.isFirstFrameSent;
import static io.rsocket.core.StateUtils.isMaxAllowedRequestN;
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
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class RequestStreamRequesterFlux extends Flux<Payload>
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
  static final AtomicLongFieldUpdater<RequestStreamRequesterFlux> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestStreamRequesterFlux.class, "state");

  int streamId;
  CoreSubscriber<? super Payload> inboundSubscriber;
  CompositeByteBuf frames;
  boolean done;

  RequestStreamRequesterFlux(
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
          actual, new IllegalStateException("RequestStreamFlux allows only a single Subscriber"));
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

    this.inboundSubscriber = actual;
    actual.onSubscribe(this);
  }

  @Override
  public final void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    long previousState = addRequestN(STATE, this, n);
    if (isTerminated(previousState)) {
      return;
    }

    if (isWorkInProgress(previousState)) {
      if (isFirstFrameSent(previousState)
          && !isMaxAllowedRequestN(extractRequestN(previousState))) {
        final ByteBuf requestNFrame = RequestNFrameCodec.encode(this.allocator, this.streamId, n);
        this.sendProcessor.onNext(requestNFrame);
      }
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

      this.inboundSubscriber.onError(Exceptions.unwrap(t));
      return;
    }

    try {
      sendReleasingPayload(
          streamId,
          FrameType.REQUEST_STREAM,
          initialRequestN,
          this.mtu,
          payload,
          sender,
          allocator,
          false);
    } catch (Throwable e) {
      this.done = true;
      lazyTerminate(STATE, this);

      sm.remove(streamId, this);

      this.inboundSubscriber.onError(e);
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

      return;
    }

    if (isMaxAllowedRequestN(initialRequestN)) {
      return;
    }

    long requestN = extractRequestN(previousState);
    if (isMaxAllowedRequestN(requestN)) {
      final ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, streamId, requestN);
      sender.onNext(requestNFrame);
      return;
    }

    if (requestN > initialRequestN) {
      final ByteBuf requestNFrame =
          RequestNFrameCodec.encode(allocator, streamId, requestN - initialRequestN);
      sender.onNext(requestNFrame);
    }
  }

  @Override
  public final void cancel() {
    final long previousState = markTerminated(STATE, this);
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

      final ByteBuf cancelFrame = CancelFrameCodec.encode(this.allocator, streamId);
      this.sendProcessor.onNext(cancelFrame);
    } else if (!isWorkInProgress(previousState)) {
      // no need to send anything, since the first request has not happened
      this.payload.release();
    }
  }

  @Override
  public final void handlePayload(Payload p) {
    if (this.done) {
      p.release();
      return;
    }

    this.inboundSubscriber.onNext(p);
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

    this.inboundSubscriber.onComplete();
  }

  @Override
  public final void handleError(Throwable cause) {
    if (this.done) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    this.done = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    this.streamManager.remove(this.streamId, this);

    final CompositeByteBuf frames = this.frames;
    this.frames = null;
    if (isReassembling(previousState)) {
      synchronized (frames) {
        frames.release();
      }
    }


    this.inboundSubscriber.onError(cause);
  }

  @Override
  public void handleNext(ByteBuf frame, boolean hasFollows, boolean isLastPayload) {
    handleNextSupport(
        STATE,
        this,
        this,
        this,
        this.inboundSubscriber,
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
    if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return extractRequestN(state);

    return null;
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(RequestStreamFlux)";
  }
}
