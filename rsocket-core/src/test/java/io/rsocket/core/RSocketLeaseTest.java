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

import static io.rsocket.frame.FrameHeaderCodec.frameType;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameType.COMPLETE;
import static io.rsocket.frame.FrameType.ERROR;
import static io.rsocket.frame.FrameType.LEASE;
import static io.rsocket.frame.FrameType.REQUEST_FNF;
import static io.rsocket.frame.FrameType.SETUP;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.TestScheduler;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.exceptions.RejectedException;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseTracker;
import io.rsocket.lease.Leases;
import io.rsocket.lease.MissingLeaseException;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class RSocketLeaseTest {
  private static final String TAG = "test";

  RSocket rSocketRequester;
  ResponderLeaseHandler responderLeaseHandler;
  LeaksTrackingByteBufAllocator byteBufAllocator;
  TestDuplexConnection clientConnection;
  TestDuplexConnection serverConnection;
  RSocketResponder rSocketResponder;
  RSocket mockRSocketHandler;

  EmitterProcessor<Lease> leaseSender = EmitterProcessor.create();
  Flux<Lease> leaseReceiver;
  RequesterLeaseHandler requesterLeaseHandler;
  TestLeaseTracker leaseTracker;

  @BeforeEach
  void setUp() {
    PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    byteBufAllocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    clientConnection = new TestDuplexConnection(byteBufAllocator);
    serverConnection = new TestDuplexConnection(byteBufAllocator);

    leaseTracker = new TestLeaseTracker();
    requesterLeaseHandler = new RequesterLeaseHandler.Impl(TAG, leases -> leaseReceiver = leases);
    responderLeaseHandler =
        new ResponderLeaseHandler.Impl<>(TAG, byteBufAllocator, stats -> leaseSender, leaseTracker);

    ClientServerInputMultiplexer clientMultiplexer =
        new ClientServerInputMultiplexer(
            clientConnection, new InitializingInterceptorRegistry(), true);

    rSocketRequester =
        new RSocketRequester(
            clientMultiplexer.asClientConnection(),
            payloadDecoder,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            0,
            0,
            null,
            requesterLeaseHandler,
            TestScheduler.INSTANCE);

    mockRSocketHandler = mock(RSocket.class);
    when(mockRSocketHandler.metadataPush(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Mono.empty();
            });
    when(mockRSocketHandler.fireAndForget(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Mono.empty();
            });
    when(mockRSocketHandler.requestResponse(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Mono.empty();
            });
    when(mockRSocketHandler.requestStream(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Flux.empty();
            });
    when(mockRSocketHandler.requestChannel(any()))
        .then(
            a -> {
              Publisher<Payload> payloadPublisher = a.getArgument(0);
              return Flux.from(payloadPublisher)
                  .doOnNext(ReferenceCounted::release)
                  .thenMany(Flux.empty());
            });

    rSocketResponder =
        new RSocketResponder(
            serverConnection,
            mockRSocketHandler,
            payloadDecoder,
            responderLeaseHandler,
            0,
            FRAME_LENGTH_MASK);
  }

  @Test
  public void serverRSocketFactoryRejectsUnsupportedLease() {
    Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
    ByteBuf setupFrame =
        SetupFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            payload);

    TestServerTransport transport = new TestServerTransport();
    RSocketServer.create().bind(transport).block();

    TestDuplexConnection connection = transport.connect();
    connection.addToReceivedBuffer(setupFrame);

    Collection<ByteBuf> sent = connection.getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf error = sent.iterator().next();
    Assertions.assertThat(frameType(error)).isEqualTo(ERROR);
    Assertions.assertThat(Exceptions.from(0, error).getMessage())
        .isEqualTo("lease is not supported");
  }

  @Test
  public void clientRSocketFactorySetsLeaseFlag() {
    TestClientTransport clientTransport = new TestClientTransport();
    RSocketConnector.create().lease(Leases::new).connect(clientTransport).block();

    Collection<ByteBuf> sent = clientTransport.testConnection().getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf setup = sent.iterator().next();
    Assertions.assertThat(frameType(setup)).isEqualTo(SETUP);
    Assertions.assertThat(SetupFrameCodec.honorLease(setup)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterMissingLeaseRequestsAreRejected(
      BiFunction<RSocket, Payload, Publisher<?>> interaction) {
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);
    StepVerifier.create(interaction.apply(rSocketRequester, payload1))
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterPresentLeaseRequestsAreAccepted(
      BiFunction<RSocket, Payload, Publisher<?>> interaction, FrameType frameType) {
    ByteBuf frame = leaseFrame(5_000, 2, Unpooled.EMPTY_BUFFER);
    requesterLeaseHandler.receive(frame);

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(1.0, offset(1e-2));
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);
    Flux.from(interaction.apply(rSocketRequester, payload1))
        .as(StepVerifier::create)
        .then(
            () -> {
              if (frameType != REQUEST_FNF) {
                clientConnection.addToReceivedBuffer(
                    PayloadFrameCodec.encodeComplete(byteBufAllocator, 1));
              }
            })
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(clientConnection.getSent())
        .hasSize(1)
        .first()
        .matches(ReferenceCounted::release);

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.5, offset(1e-2));

    Assertions.assertThat(frame.release()).isTrue();

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  @SuppressWarnings({"rawtypes", "unchecked"})
  void requesterDepletedAllowedLeaseRequestsAreRejected(
      BiFunction<RSocket, Payload, Publisher<?>> interaction, FrameType interactionType) {
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);
    ByteBuf leaseFrame = leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER);
    requesterLeaseHandler.receive(leaseFrame);

    double initialAvailability = requesterLeaseHandler.availability();
    Publisher<?> request = interaction.apply(rSocketRequester, payload1);

    // ensures that lease is not used until the frame is sent
    Assertions.assertThat(initialAvailability).isEqualTo(requesterLeaseHandler.availability());
    Assertions.assertThat(clientConnection.getSent()).hasSize(0);

    AssertSubscriber assertSubscriber = AssertSubscriber.create(0);
    request.subscribe(assertSubscriber);

    // if request is FNF, then request frame is sent on subscribe
    // otherwise we need to make request(1)
    if (interactionType != REQUEST_FNF) {
      Assertions.assertThat(initialAvailability).isEqualTo(requesterLeaseHandler.availability());
      Assertions.assertThat(clientConnection.getSent()).hasSize(0);

      assertSubscriber.request(1);
    }

    // ensures availability is changed and lease is used only up on frame sending
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
    Assertions.assertThat(clientConnection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> frameType(bb) == interactionType)
        .matches(ReferenceCounted::release);

    ByteBuf buffer2 = byteBufAllocator.buffer();
    buffer2.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload2 = ByteBufPayload.create(buffer2);
    Flux.from(interaction.apply(rSocketRequester, payload2))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(leaseFrame.release()).isTrue();

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterExpiredLeaseRequestsAreRejected(
      BiFunction<RSocket, Payload, Publisher<?>> interaction) {
    ByteBuf frame = leaseFrame(50, 1, Unpooled.EMPTY_BUFFER);
    requesterLeaseHandler.receive(frame);

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    Flux.defer(() -> interaction.apply(rSocketRequester, payload1))
        .delaySubscription(Duration.ofMillis(200))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(frame.release()).isTrue();

    byteBufAllocator.assertHasNoLeaks();
  }

  @Test
  void requesterAvailabilityRespectsTransport() {
    requesterLeaseHandler.receive(leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER));
    double unavailable = 0.0;
    clientConnection.setAvailability(unavailable);
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(unavailable, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("rawInteractions")
  void responderMissingLeaseRequestsAreRejected(
      BiConsumer<DuplexConnection, Payload> interaction, FrameType requestType) {
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    Flux.from(serverConnection.getSentAsPublisher())
        .take(1)
        .as(StepVerifier::create)
        .then(() -> interaction.accept(serverConnection, payload1))
        .assertNext(
            bb -> {
              Assertions.assertThat(frameType(bb)).isEqualTo(ERROR);
              Assertions.assertThat(Exceptions.from(1, bb))
                  .isInstanceOf(RejectedException.class)
                  .hasMessage("[test] Lease was not received yet");
            })
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(leaseTracker.rejectedStreams)
        .hasSize(1)
        .containsOnly(Tuples.of(1, requestType));
  }

  @ParameterizedTest
  @MethodSource("rawInteractions")
  void responderPresentLeaseRequestsAreAccepted(
      BiConsumer<DuplexConnection, Payload> interaction, FrameType frameType) {

    leaseSender.onNext(Lease.create(5_000, 2));

    MonoProcessor<Payload> response = MonoProcessor.create();

    Mockito.reset(mockRSocketHandler);

    switch (frameType) {
      case REQUEST_RESPONSE:
        when(mockRSocketHandler.requestResponse(any())).thenReturn(response);
        break;
      case REQUEST_STREAM:
        when(mockRSocketHandler.requestStream(any())).thenReturn(response.flux());
        break;
      case REQUEST_CHANNEL:
        when(mockRSocketHandler.requestChannel(any())).thenReturn(response.flux());
        break;
    }

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    Flux.from(serverConnection.getSentAsPublisher())
        .take(1)
        .as(StepVerifier::create)
        .then(() -> interaction.accept(serverConnection, payload1))
        .then(
            () -> {
              Assertions.assertThat(leaseTracker.acceptedStreams)
                  .containsOnly(Tuples.of(1, frameType));
              Assertions.assertThat(leaseTracker.rejectedStreams).isEmpty();
              Assertions.assertThat(leaseTracker.releasedStreams).isEmpty();

              response.onComplete();

              Assertions.assertThat(leaseTracker.acceptedStreams)
                  .containsOnly(Tuples.of(1, frameType));
              Assertions.assertThat(leaseTracker.rejectedStreams).isEmpty();
              Assertions.assertThat(leaseTracker.releasedStreams)
                  .containsOnly(Tuples.of(1, SignalType.ON_COMPLETE));
            })
        .assertNext(bb -> Assertions.assertThat(frameType(bb)).isEqualTo(COMPLETE))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    switch (frameType) {
      case REQUEST_RESPONSE:
        Mockito.verify(mockRSocketHandler).requestResponse(any());
        break;
      case REQUEST_STREAM:
        Mockito.verify(mockRSocketHandler).requestStream(any());
        break;
      case REQUEST_CHANNEL:
        Mockito.verify(mockRSocketHandler).requestChannel(any());
        break;
    }

    Assertions.assertThat(serverConnection.getSent())
        .hasSize(2)
        .first()
        .matches(bb -> frameType(bb) == LEASE)
        .matches(ReferenceCounted::release);

    Assertions.assertThat(serverConnection.getSent())
        .element(1)
        .matches(bb -> frameType(bb) == COMPLETE)
        .matches(ReferenceCountUtil::release);

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("rawInteractions")
  void expiredLeaseRequestsAreRejected(
      BiConsumer<DuplexConnection, Payload> interaction, FrameType frameType)
      throws InterruptedException {
    leaseSender.onNext(Lease.create(50, 1));

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    Thread.sleep(100);

    Flux.from(serverConnection.getSentAsPublisher())
        .take(1)
        .as(StepVerifier::create)
        .then(() -> interaction.accept(serverConnection, payload1))
        .assertNext(
            bb -> {
              Assertions.assertThat(frameType(bb)).isEqualTo(ERROR);
              Assertions.assertThat(Exceptions.from(1, bb))
                  .isInstanceOf(RejectedException.class)
                  .hasMessage("[test] Missing leases. Expired: true, allowedRequests: 1");
            })
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(serverConnection.getSent())
        .hasSize(2)
        .first()
        .matches(bb -> frameType(bb) == LEASE)
        .matches(ReferenceCounted::release);

    Assertions.assertThat(serverConnection.getSent())
        .hasSize(2)
        .element(1)
        .matches(bb -> frameType(bb) == ERROR)
        .matches(ReferenceCounted::release);

    byteBufAllocator.assertHasNoLeaks();
  }

  @Test
  void sendLease() {
    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;
    leaseSender.onNext(Lease.create(5_000, 2, metadata));

    ByteBuf leaseFrame =
        serverConnection
            .getSent()
            .stream()
            .filter(f -> frameType(f) == FrameType.LEASE)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Lease frame not sent"));

    Assertions.assertThat(LeaseFrameCodec.ttl(leaseFrame)).isEqualTo(ttl);
    Assertions.assertThat(LeaseFrameCodec.numRequests(leaseFrame)).isEqualTo(numberOfRequests);
    Assertions.assertThat(LeaseFrameCodec.metadata(leaseFrame).toString(utf8))
        .isEqualTo(metadataContent);
  }

  @Test
  void receiveLease() {
    Collection<Lease> receivedLeases = new ArrayList<>();
    leaseReceiver.subscribe(lease -> receivedLeases.add(lease));

    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;

    ByteBuf leaseFrame = leaseFrame(ttl, numberOfRequests, metadata).retain(1);

    clientConnection.addToReceivedBuffer(leaseFrame);

    Assertions.assertThat(receivedLeases.isEmpty()).isFalse();
    Lease receivedLease = receivedLeases.iterator().next();
    Assertions.assertThat(receivedLease.getTimeToLiveMillis()).isEqualTo(ttl);
    Assertions.assertThat(receivedLease.getStartingAllowedRequests()).isEqualTo(numberOfRequests);
    Assertions.assertThat(receivedLease.getMetadata().toString(utf8)).isEqualTo(metadataContent);
  }

  ByteBuf leaseFrame(int ttl, int requests, ByteBuf metadata) {
    return LeaseFrameCodec.encode(byteBufAllocator, ttl, requests, metadata);
  }

  static Stream<Arguments> interactions() {
    return Stream.of(
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>) RSocket::fireAndForget,
            FrameType.REQUEST_FNF),
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>) RSocket::requestResponse,
            FrameType.REQUEST_RESPONSE),
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>) RSocket::requestStream,
            FrameType.REQUEST_STREAM),
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>)
                (rSocket, payload) -> rSocket.requestChannel(Mono.just(payload)),
            FrameType.REQUEST_CHANNEL));
  }

  static Stream<Arguments> rawInteractions() {
    return Stream.of(
        Arguments.of(
            (BiConsumer<TestDuplexConnection, Payload>)
                (c, p) ->
                    c.addToReceivedBuffer(
                        RequestResponseFrameCodec.encodeReleasingPayload(c.alloc(), 1, p)),
            FrameType.REQUEST_RESPONSE),
        Arguments.of(
            (BiConsumer<TestDuplexConnection, Payload>)
                (c, p) ->
                    c.addToReceivedBuffer(
                        RequestStreamFrameCodec.encodeReleasingPayload(
                            c.alloc(), 1, Integer.MAX_VALUE, p)),
            FrameType.REQUEST_STREAM),
        Arguments.of(
            (BiConsumer<TestDuplexConnection, Payload>)
                (c, p) ->
                    c.addToReceivedBuffer(
                        RequestChannelFrameCodec.encodeReleasingPayload(
                            c.alloc(), 1, false, Integer.MAX_VALUE, p)),
            FrameType.REQUEST_CHANNEL));
  }

  private class TestLeaseTracker implements LeaseTracker {

    final Queue<Tuple2<Integer, FrameType>> acceptedStreams = new LinkedBlockingQueue<>();
    final Queue<Tuple2<Integer, FrameType>> rejectedStreams = new LinkedBlockingQueue<>();
    final Queue<Tuple2<Integer, SignalType>> releasedStreams = new LinkedBlockingQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public void onAccept(int streamId, FrameType requestType, ByteBuf metadata) {
      acceptedStreams.offer(Tuples.of(streamId, requestType));
    }

    @Override
    public void onReject(int streamId, FrameType requestType, ByteBuf metadata) {
      rejectedStreams.offer(Tuples.of(streamId, requestType));
    }

    @Override
    public void onRelease(int streamId, SignalType terminalSignal) {
      releasedStreams.offer(Tuples.of(streamId, terminalSignal));
    }

    @Override
    public void onClose() {
      closed.set(true);
    }
  }
}
