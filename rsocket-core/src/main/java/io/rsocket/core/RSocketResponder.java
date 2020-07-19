/*
 * Copyright 2015-2018 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.ResponderRSocket;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.lease.ResponderLeaseHandler;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder implements RSocket, StreamManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketResponder.class);

  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  private final DuplexConnection connection;
  private final RSocket requestHandler;

  @SuppressWarnings("deprecation")
  private final io.rsocket.ResponderRSocket responderRSocket;

  private final PayloadDecoder payloadDecoder;
  private final ResponderLeaseHandler leaseHandler;
  private final Disposable leaseHandlerDisposable;

  private volatile Throwable terminationError;
  private static final AtomicReferenceFieldUpdater<RSocketResponder, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketResponder.class, Throwable.class, "terminationError");

  private final int mtu;
  private final int maxFrameLength;
  private final int maxInboundPayloadSize;

  private final IntObjectMap<FrameHandler> activeStreams;
  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;

  RSocketResponder(
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      ResponderLeaseHandler leaseHandler,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize) {
    this.connection = connection;
    this.allocator = connection.alloc();
    this.mtu = mtu;
    this.maxFrameLength = maxFrameLength;
    this.maxInboundPayloadSize = maxInboundPayloadSize;

    this.requestHandler = requestHandler;
    this.responderRSocket =
        (requestHandler instanceof io.rsocket.ResponderRSocket)
            ? (io.rsocket.ResponderRSocket) requestHandler
            : null;

    this.payloadDecoder = payloadDecoder;
    this.leaseHandler = leaseHandler;
    this.activeStreams = new SynchronizedIntObjectHashMap<>();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();

    connection.send(sendProcessor).subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleFrame, e -> {});
    leaseHandlerDisposable = leaseHandler.send(sendProcessor::onNextPrioritized);

    this.connection
        .onClose()
        .subscribe(null, this::tryTerminateOnConnectionError, this::tryTerminateOnConnectionClose);
  }

  private void handleSendProcessorError(Throwable t) {
    for (FrameHandler frameHandler : activeStreams.values()) {
      frameHandler.handleError(t);
    }
  }

  private void tryTerminateOnConnectionError(Throwable e) {
    tryTerminate(() -> e);
  }

  private void tryTerminateOnConnectionClose() {
    tryTerminate(() -> CLOSED_CHANNEL_EXCEPTION);
  }

  private void tryTerminate(Supplier<Throwable> errorSupplier) {
    if (terminationError == null) {
      Throwable e = errorSupplier.get();
      if (TERMINATION_ERROR.compareAndSet(this, null, e)) {
        cleanup(e);
      }
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.fireAndForget(payload);
      } else {
        payload.release();
        return Mono.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestResponse(payload);
      } else {
        payload.release();
        return Mono.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestStream(payload);
      } else {
        payload.release();
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestChannel(payloads);
      } else {
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  private Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    try {
      if (leaseHandler.useLease()) {
        final ResponderRSocket responderRSocket = this.responderRSocket;
        if (responderRSocket != null) {
          return responderRSocket.requestChannel(payload, payloads);
        } else {
          return requestHandler.requestChannel(payloads);
        }
      } else {
        payload.release();
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return requestHandler.metadataPush(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public void dispose() {
    tryTerminate(() -> new CancellationException("Disposed"));
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  private void cleanup(Throwable e) {
    cleanUpSendingSubscriptions();

    connection.dispose();
    leaseHandlerDisposable.dispose();
    requestHandler.dispose();
    sendProcessor.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    activeStreams.values().forEach(FrameHandler::handleCancel);
    activeStreams.clear();
  }

  private void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderCodec.streamId(frame);
      FrameHandler receiver;
      FrameType frameType = FrameHeaderCodec.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, frame);
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, frame);
          break;
        case REQUEST_STREAM:
          long streamInitialRequestN = RequestStreamFrameCodec.initialRequestN(frame);
          handleStream(streamId, frame, streamInitialRequestN);
          break;
        case REQUEST_CHANNEL:
          long channelInitialRequestN = RequestChannelFrameCodec.initialRequestN(frame);
          handleChannel(streamId, frame, channelInitialRequestN);
          break;
        case METADATA_PUSH:
          handleMetadataPush(metadataPush(payloadDecoder.apply(frame)));
          break;
        case CANCEL:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            receiver.handleCancel();
          }
          break;
        case REQUEST_N:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            long n = RequestNFrameCodec.requestN(frame);
            receiver.handleRequestN(n);
          }
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case NEXT:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            boolean hasFollows = FrameHeaderCodec.hasFollows(frame);
            receiver.handleNext(frame, hasFollows, false);
          }
          break;
        case COMPLETE:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            receiver.handleComplete();
          }
          break;
        case ERROR:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            receiver.handleError(io.rsocket.exceptions.Exceptions.from(streamId, frame));
          }
          break;
        case NEXT_COMPLETE:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            receiver.handleNext(frame, false, true);
          }
          break;
        case SETUP:
          sendProcessor.onNext(
              ErrorFrameCodec.encode(
                  allocator,
                  streamId,
                  new IllegalStateException("Setup frame received post setup.")));
          break;
        case LEASE:
        default:
          sendProcessor.onNext(
              ErrorFrameCodec.encode(
                  allocator,
                  streamId,
                  new IllegalStateException("ServerRSocket: Unexpected frame type: " + frameType)));
          break;
      }
      ReferenceCountUtil.safeRelease(frame);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw Exceptions.propagate(t);
    }
  }

  private void handleFireAndForget(int streamId, ByteBuf frame) {
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderCodec.hasFollows(frame)) {
        FireAndForgetResponderSubscriber subscriber =
            new FireAndForgetResponderSubscriber(
                streamId, frame, allocator, payloadDecoder, Integer.MAX_VALUE, this, this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.handleCancel();
        }
      } else {
        fireAndForget(this.payloadDecoder.apply(frame))
            .subscribe(FireAndForgetResponderSubscriber.INSTANCE);
      }
    }
  }

  private void handleRequestResponse(int streamId, ByteBuf frame) {
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderCodec.hasFollows(frame)) {
        RequestResponseResponderSubscriber subscriber =
            new RequestResponseResponderSubscriber(
                streamId,
                this.allocator,
                this.payloadDecoder,
                frame,
                this.mtu,
                this.maxFrameLength,
                this.maxInboundPayloadSize,
                this,
                this.sendProcessor,
                this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.handleCancel();
        }
      } else {
        RequestResponseResponderSubscriber subscriber =
            new RequestResponseResponderSubscriber(
                streamId,
                this.allocator,
                this.mtu,
                this.maxFrameLength,
                this.maxInboundPayloadSize,
                this,
                this.sendProcessor);
        if (activeStreams.putIfAbsent(streamId, subscriber) == null) {
          this.requestResponse(payloadDecoder.apply(frame)).subscribe(subscriber);
        }
      }
    }
  }

  private void handleStream(int streamId, ByteBuf frame, long initialRequestN) {
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderCodec.hasFollows(frame)) {
        RequestStreamResponderSubscriber subscriber =
            new RequestStreamResponderSubscriber(
                streamId,
                initialRequestN,
                this.allocator,
                this.payloadDecoder,
                frame,
                this.mtu,
                this.maxFrameLength,
                this.maxInboundPayloadSize,
                this,
                this.sendProcessor,
                this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.handleCancel();
        }
      } else {
        RequestStreamResponderSubscriber subscriber =
            new RequestStreamResponderSubscriber(
                streamId,
                initialRequestN,
                this.allocator,
                this.mtu,
                this.maxFrameLength,
                this.maxInboundPayloadSize,
                this,
                this.sendProcessor);
        if (activeStreams.putIfAbsent(streamId, subscriber) == null) {
          this.requestStream(payloadDecoder.apply(frame)).subscribe(subscriber);
        }
      }
    }
  }

  private void handleChannel(int streamId, ByteBuf frame, long initialRequestN) {
    final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderCodec.hasFollows(frame)) {
        RequestChannelResponderSubscriber subscriber =
            new RequestChannelResponderSubscriber(
                streamId,
                initialRequestN,
                this.allocator,
                this.payloadDecoder,
                frame,
                this.mtu,
                this.maxFrameLength,
                this.maxInboundPayloadSize,
                this,
                this.sendProcessor,
                this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.handleCancel();
        }
      } else {
        final Payload firstPayload = this.payloadDecoder.apply(frame);
        RequestChannelResponderSubscriber subscriber =
            new RequestChannelResponderSubscriber(
                streamId,
                initialRequestN == Integer.MAX_VALUE ? Long.MAX_VALUE : initialRequestN,
                this.allocator,
                this.payloadDecoder,
                firstPayload,
                this.mtu,
                this.maxFrameLength,
                this.maxInboundPayloadSize,
                this,
                this.sendProcessor);
        if (activeStreams.putIfAbsent(streamId, subscriber) == null) {
          this.requestChannel(firstPayload, subscriber).subscribe(subscriber);
        } else {
          firstPayload.release();
        }
      }
    }
  }

  private void handleMetadataPush(Mono<Void> result) {
    result.subscribe(MetadataPushResponderSubscriber.INSTANCE);
  }

  @Override
  public int getNextId() {
    throw new UnsupportedOperationException("Responder can not issue id");
  }

  @Override
  public int addAndGetNextId(FrameHandler frameHandler) {
    throw new UnsupportedOperationException("Responder can not issue id");
  }

  @Override
  public FrameHandler get(int streamId) {
    return this.activeStreams.get(streamId);
  }

  @Override
  public boolean remove(int streamId, FrameHandler frameHandler) {
    return this.activeStreams.remove(streamId, frameHandler);
  }
}
