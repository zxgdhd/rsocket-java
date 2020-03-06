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
import static io.rsocket.core.SendUtils.sendReleasingPayload;
import static io.rsocket.core.StateUtils.REASSEMBLING_FLAG;
import static io.rsocket.core.StateUtils.isFirstFrameSent;
import static io.rsocket.core.StateUtils.isInboundTerminated;
import static io.rsocket.core.StateUtils.isMaxAllowedRequestN;
import static io.rsocket.core.StateUtils.isOutboundTerminated;
import static io.rsocket.core.StateUtils.isReassembling;
import static io.rsocket.core.StateUtils.isSubscribed;
import static io.rsocket.core.StateUtils.isTerminated;
import static io.rsocket.core.StateUtils.isWorkInProgress;
import static io.rsocket.core.StateUtils.markFirstFrameSent;
import static io.rsocket.core.StateUtils.markInboundTerminated;
import static io.rsocket.core.StateUtils.markOutboundTerminated;
import static io.rsocket.core.StateUtils.markReassembled;
import static io.rsocket.core.StateUtils.markReassembling;
import static io.rsocket.core.StateUtils.markSubscribed;
import static io.rsocket.core.StateUtils.markTerminated;
import static reactor.core.Exceptions.TERMINATED;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.CanceledException;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

final class RequestChannelResponderSubscriber extends Flux<Payload>
    implements ResponderFrameHandler, Subscription, CoreSubscriber<Payload> {

  static final Logger logger = LoggerFactory.getLogger(RequestChannelResponderSubscriber.class);

  final int streamId;
  final ByteBufAllocator allocator;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final StreamManager streamManager;
  final UnboundedProcessor<ByteBuf> sendProcessor;
  final long firstRequest;

  final RSocket handler;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestChannelResponderSubscriber> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestChannelResponderSubscriber.class, "state");

  CompositeByteBuf frames;
  Payload firstPayload;

  Subscription outboundSubscription;
  CoreSubscriber<? super Payload> inboundSubscriber;

  volatile Throwable inboundError;
  static final AtomicReferenceFieldUpdater<RequestChannelResponderSubscriber, Throwable>
      INBOUND_ERROR =
          AtomicReferenceFieldUpdater.newUpdater(
              RequestChannelResponderSubscriber.class, Throwable.class, "inboundError");

  boolean inboundDone;
  boolean outboundDone;

  public RequestChannelResponderSubscriber(
      int streamId,
      long firstRequest,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      ByteBuf firstFrame,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize,
      StreamManager streamManager,
      UnboundedProcessor<ByteBuf> sendProcessor,
      RSocket handler) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.mtu = mtu;
    this.maxFrameLength = maxFrameLength;
    this.maxInboundPayloadSize = maxInboundPayloadSize;
    this.streamManager = streamManager;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
    this.handler = handler;
    this.firstRequest = firstRequest;

    this.frames =
        ReassemblyUtils.addFollowingFrame(
            allocator.compositeBuffer(), firstFrame, maxInboundPayloadSize);
    STATE.lazySet(this, REASSEMBLING_FLAG);
  }

  public RequestChannelResponderSubscriber(
      int streamId,
      long firstRequest,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      Payload firstPayload,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize,
      StreamManager streamManager,
      UnboundedProcessor<ByteBuf> sendProcessor) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.mtu = mtu;
    this.maxFrameLength = maxFrameLength;
    this.maxInboundPayloadSize = maxInboundPayloadSize;
    this.streamManager = streamManager;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
    this.firstRequest = firstRequest;
    this.firstPayload = firstPayload;

    this.handler = null;
    this.frames = null;
  }

  @Override
  // subscriber from the requestChannel method
  public void subscribe(CoreSubscriber<? super Payload> actual) {

    long previousState = markSubscribed(STATE, this);
    if (isTerminated(previousState)) {
      Throwable t = this.inboundError;
      if (t != null) {
        Operators.error(actual, t);
      } else {
        Operators.error(
            actual,
            new CancellationException("RequestChannelSubscriber has already been terminated"));
      }
      return;
    }

    if (isSubscribed(previousState)) {
      Operators.error(
          actual, new IllegalStateException("RequestChannelSubscriber allows only one Subscriber"));
      return;
    }

    this.inboundSubscriber = actual;
    // sends sender as a subscription since every request|cancel signal should be encoded to
    // requestNFrame|cancelFrame
    actual.onSubscribe(this);
  }

  @Override
  // subscription to the outbound
  public void onSubscribe(Subscription outboundSubscription) {
    if (Operators.validate(this.outboundSubscription, outboundSubscription)) {
      this.outboundSubscription = outboundSubscription;
      outboundSubscription.request(this.firstRequest);
    }
  }

  @Override
  // request to the inbound
  public void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    long previousState = StateUtils.addRequestN(STATE, this, n);
    if (isTerminated(previousState) || isInboundTerminated(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError == TERMINATED) {
        return;
      }

      if (inboundError == null && this.inboundDone) {
        this.inboundSubscriber.onComplete();
      } else if (inboundError != null) {
        this.inboundSubscriber.onError(inboundError);
      }

      return;
    }

    if (isWorkInProgress(previousState)) {
      if (isFirstFrameSent(previousState)
          && !isMaxAllowedRequestN(StateUtils.extractRequestN(previousState))) {
        final ByteBuf requestNFrame = RequestNFrameCodec.encode(this.allocator, this.streamId, n);
        this.sendProcessor.onNext(requestNFrame);
      }
      return;
    }

    final Payload firstPayload = this.firstPayload;
    this.firstPayload = null;
    this.inboundSubscriber.onNext(firstPayload);

    previousState = markFirstFrameSent(STATE, this);
    if (isInboundTerminated(previousState) || isTerminated(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError == TERMINATED) {
        return;
      }

      if (inboundError == null && this.inboundDone) {
        this.inboundSubscriber.onComplete();
      } else if (inboundError != null) {
        this.inboundSubscriber.onError(inboundError);
      }
      return;
    }

    long requestN = StateUtils.extractRequestN(previousState);
    if (isMaxAllowedRequestN(requestN)) {
      final ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, streamId, requestN);
      this.sendProcessor.onNext(requestNFrame);
    } else {
      long firstRequestN = requestN - 1;
      if (firstRequestN > 0) {
        final ByteBuf requestNFrame =
            RequestNFrameCodec.encode(this.allocator, this.streamId, firstRequestN);
        this.sendProcessor.onNext(requestNFrame);
      }
    }
  }

  @Override
  // inbound cancellation
  public void cancel() {
    long previousState = markInboundTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    if (!isFirstFrameSent(previousState) && !isWorkInProgress(previousState)) {
      final Payload firstPayload = this.firstPayload;
      this.firstPayload = null;
      firstPayload.release();
    }

    final int streamId = this.streamId;

    if (isOutboundTerminated(previousState)) {
      this.streamManager.remove(streamId, this);
    }

    final ByteBuf cancelFrame = CancelFrameCodec.encode(this.allocator, streamId);
    this.sendProcessor.onNext(cancelFrame);
  }

  @Override
  public final void handleCancel() {

    Exceptions.addThrowable(
        INBOUND_ERROR, this, new CancellationException("Inbound has been canceled"));

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    final CompositeByteBuf frames = this.frames;
    this.frames = null;
    if (isReassembling(previousState)) {
      frames.release();
    }

    this.streamManager.remove(this.streamId, this);

    if (this.outboundSubscription != null) {
      this.outboundSubscription.cancel();
    }

    if (isFirstFrameSent(previousState)) {
      if (!isInboundTerminated(previousState)) {
        Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
        if (inboundError != TERMINATED) {
          //noinspection ConstantConditions
          this.inboundSubscriber.onError(inboundError);
        }
      }
    } else {
      this.firstPayload.release();
    }
  }

  final void handlePayload(Payload p) {
    if (this.inboundDone) {
      // payload from network so it has refCnt > 0
      p.release();
      return;
    }

    this.inboundSubscriber.onNext(p);
  }

  @Override
  public final void handleError(Throwable t) {
    if (this.inboundDone) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    this.inboundDone = true;
    Exceptions.addThrowable(INBOUND_ERROR, this, t);

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    this.streamManager.remove(this.streamId, this);

    final CompositeByteBuf frames = this.frames;
    this.frames = null;
    if (isReassembling(previousState)) {
      frames.release();
    }

    if (!isWorkInProgress(previousState)) {
      if (!isFirstFrameSent(previousState)) {
        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;
        firstPayload.release();
      }

      if (isSubscribed(previousState)) {
        Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
        if (inboundError != TERMINATED) {
          //noinspection ConstantConditions
          this.inboundSubscriber.onError(inboundError);
        }
      }
    } else if (isFirstFrameSent(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError != TERMINATED) {
        //noinspection ConstantConditions
        this.inboundSubscriber.onError(inboundError);
      }
    }

    // this is downstream subscription so need to cancel it just in case error signal has not
    // reached it
    // needs for disconnected upstream and downstream case
    this.outboundSubscription.cancel();
  }

  @Override
  public void handleComplete() {
    if (this.inboundDone) {
      return;
    }

    this.inboundDone = true;

    long previousState = markInboundTerminated(STATE, this);

    if (isOutboundTerminated(previousState)) {
      this.streamManager.remove(this.streamId, this);
    }

    if (isFirstFrameSent(previousState)) {
      this.inboundSubscriber.onComplete();
    }
  }

  @Override
  public void handleNext(ByteBuf frame, boolean hasFollows, boolean isLastPayload) {
    long state = this.state;
    if (isTerminated(state)) {
      return;
    }

    if (!hasFollows && !isReassembling(state)) {
      Payload payload;
      try {
        payload = this.payloadDecoder.apply(frame);
      } catch (Throwable t) {
        this.handleCancel();

        this.outboundDone = true;
        // send error to terminate interaction
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                this.allocator, this.streamId, new CanceledException(t.getMessage()));
        this.sendProcessor.onNext(errorFrame);

        return;
      }

      this.handlePayload(payload);
      if (isLastPayload) {
        this.handleComplete();
      }
      return;
    }

    CompositeByteBuf frames = this.frames;
    if (frames == null) {
      frames =
          ReassemblyUtils.addFollowingFrame(
              this.allocator.compositeBuffer(), frame, this.maxInboundPayloadSize);
      this.frames = frames;

      long previousState = markReassembling(STATE, this);
      if (isTerminated(previousState)) {
        this.frames = null;
        frames.release();
        return;
      }
    } else {
      try {
        frames = ReassemblyUtils.addFollowingFrame(frames, frame, this.maxInboundPayloadSize);
      } catch (IllegalReferenceCountException e) {
        if (isTerminated(this.state)) {
          return;
        }

        this.handleCancel();

        this.outboundDone = true;
        // send error to terminate interaction
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                this.allocator,
                this.streamId,
                new CanceledException("Failed to reassemble payload. Cause: " + e.getMessage()));
        this.sendProcessor.onNext(errorFrame);

        return;
      }
    }

    if (!hasFollows) {
      long previousState = markReassembled(STATE, this);
      this.frames = null;

      if (isTerminated(previousState)) {
        return;
      }

      Payload payload;
      try {
        payload = this.payloadDecoder.apply(frames);
        frames.release();
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);

        this.handleCancel();
        // send error to terminate interaction
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                this.allocator,
                this.streamId,
                new CanceledException("Failed to reassemble payload. Cause: " + t.getMessage()));
        this.sendProcessor.onNext(errorFrame);

        return;
      }

      if (this.outboundSubscription == null) {
        this.firstPayload = payload;
        Flux<Payload> source = this.handler.requestChannel(this);
        source.subscribe(this);
      } else {
        this.handlePayload(payload);
      }

      if (isLastPayload) {
        this.handleComplete();
      }
    }
  }

  @Override
  public void onNext(Payload p) {
    if (this.outboundDone) {
      ReferenceCountUtil.safeRelease(p);
      return;
    }

    final int streamId = this.streamId;
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;

    if (p == null) {
      final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(allocator, streamId);
      sender.onNext(completeFrame);
      return;
    }

    final int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, p, false)) {
        p.release();

        // FIXME: must be scheduled on the connection event-loop to achieve serial
        //  behaviour on the inbound subscriber
        handleCancel();

        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                allocator,
                streamId,
                new CanceledException(
                    String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        sender.onNext(errorFrame);
        return;
      }
    } catch (IllegalReferenceCountException e) {

      // FIXME: must be scheduled on the connection event-loop to achieve serial
      //  behaviour on the inbound subscriber
      handleCancel();

      final ByteBuf errorFrame =
          ErrorFrameCodec.encode(
              allocator,
              streamId,
              new CanceledException("Failed to validate payload. Cause:" + e.getMessage()));
      sender.onNext(errorFrame);
      return;
    }

    try {
      sendReleasingPayload(streamId, FrameType.NEXT, mtu, p, sender, allocator, false);
    } catch (Throwable t) {
      // FIXME: must be scheduled on the connection event-loop to achieve serial
      //  behaviour on the inbound subscriber
      handleCancel();
    }
  }

  @Override
  public void onError(Throwable t) {
    if (this.outboundDone) {
      logger.debug("Dropped error", t);
      return;
    }

    boolean wasThrowableAdded =
        Exceptions.addThrowable(
            INBOUND_ERROR,
            this,
            new CancellationException("Outbound has terminated with an error"));
    this.outboundDone = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      logger.debug("Dropped error", t);
      return;
    }

    final int streamId = this.streamId;

    this.streamManager.remove(streamId, this);

    final CompositeByteBuf frames = this.frames;
    this.frames = null;
    if (isReassembling(previousState)) {
      frames.release();
    }

    if (!isFirstFrameSent(previousState)) {
      if (!isWorkInProgress(previousState)) {
        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;
        firstPayload.release();
      }
    }

    if (wasThrowableAdded && !isInboundTerminated(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError != TERMINATED) {
        // FIXME: must be scheduled on the connection event-loop to achieve serial
        //  behaviour on the inbound subscriber
        //noinspection ConstantConditions
        this.inboundSubscriber.onError(inboundError);
      }
    }

    final ByteBuf errorFrame = ErrorFrameCodec.encode(this.allocator, streamId, t);
    this.sendProcessor.onNext(errorFrame);
  }

  @Override
  public void onComplete() {
    if (this.outboundDone) {
      return;
    }

    this.outboundDone = true;

    long previousState = markOutboundTerminated(STATE, this, false);
    if (isTerminated(previousState)) {
      return;
    }

    final int streamId = this.streamId;

    if (isInboundTerminated(previousState)) {
      this.streamManager.remove(streamId, this);
    }

    final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(this.allocator, streamId);
    this.sendProcessor.onNext(completeFrame);
  }

  @Override
  public final void handleRequestN(long n) {
    this.outboundSubscription.request(n);
  }

  @Override
  public Context currentContext() {
    return SendUtils.DISCARD_CONTEXT;
  }
}
