package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA;
import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class RequestResponseRequesterMonoTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /*
   * +-------------------------------+
   * |      General Test Cases       |
   * +-------------------------------+
   *
   */

  /**
   * General StateMachine transition test. No Fragmentation enabled In this test we check that the
   * given instance of RequestResponseMono: 1) subscribes 2) sends frame on the first request 3)
   * terminates up on receiving the first signal (terminates on first next | error | next over
   * reassembly | complete)
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameShouldBeSentOnSubscription(
      BiFunction<RequestResponseRequesterMono, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final TestStreamManager activeStreams = TestStreamManager.client();

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(
            RequestResponseRequesterMono.STATE, requestResponseRequesterMono);

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestResponseRequesterMono,
            StepVerifier.create(requestResponseRequesterMono, 0)
                .expectSubscription()
                .then(stateMachineAssert::hasSubscribedFlagOnly)
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(activeStreams::assertNoActiveStreams)
                .thenRequest(1)
                .then(
                    () ->
                        stateMachineAssert
                            .hasSubscribedFlag()
                            .hasRequestN(1)
                            .hasFirstFrameSentFlag())
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestResponseRequesterMono)))
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();
    // should not add anything to map
    activeStreams.assertNoActiveStreams();

    final ByteBuf frame = sender.poll();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    stateMachineAssert.isTerminated();

    if (!sender.isEmpty()) {
      ByteBuf cancelFrame = sender.poll();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();
    }
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<BiFunction<RequestResponseRequesterMono, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnSubscriptionResponses() {
    return Stream.of(
        // next case
        (rrm, sv) ->
            sv.then(() -> rrm.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .expectComplete(),
        // complete case
        (rrm, sv) -> sv.then(rrm::handleComplete).expectComplete(),
        // error case
        (rrm, sv) ->
            sv.then(() -> rrm.handleError(new ApplicationErrorException("test")))
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        // fragmentation case
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
              StateMachineAssert.assertThat(rrm);

          return sv.then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFirstFragment(
                            rrm.allocator,
                            64,
                            FrameType.REQUEST_RESPONSE,
                            1,
                            payload.hasMetadata(),
                            payload.metadata(),
                            payload.data());
                    rrm.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      stateMachineAssert
                          .hasSubscribedFlag()
                          .hasRequestN(1)
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rrm.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rrm.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      stateMachineAssert
                          .hasSubscribedFlag()
                          .hasRequestN(1)
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rrm.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rrm.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      stateMachineAssert
                          .hasSubscribedFlag()
                          .hasRequestN(1)
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rrm.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rrm.handleNext(followingFrame, false, false);
                    followingFrame.release();
                  })
              .then(stateMachineAssert::isTerminated)
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    p.release();
                  })
              .then(payload::release)
              .expectComplete();
        },
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
              StateMachineAssert.assertThat(rrm);

          ByteBuf[] fragments =
              new ByteBuf[] {
                FragmentationUtils.encodeFirstFragment(
                    rrm.allocator,
                    64,
                    FrameType.REQUEST_RESPONSE,
                    1,
                    payload.hasMetadata(),
                    payload.metadata(),
                    payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    rrm.allocator, 64, 1, false, payload.metadata(), payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    rrm.allocator, 64, 1, false, payload.metadata(), payload.data())
              };

          final StepVerifier stepVerifier =
              sv.then(
                      () -> {
                        rrm.handleNext(fragments[0], true, false);
                        fragments[0].release();
                      })
                  .then(
                      () ->
                          stateMachineAssert
                              .hasSubscribedFlag()
                              .hasRequestN(1)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rrm.handleNext(fragments[1], true, false);
                        fragments[1].release();
                      })
                  .then(
                      () ->
                          stateMachineAssert
                              .hasSubscribedFlag()
                              .hasRequestN(1)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rrm.handleNext(fragments[2], true, false);
                        fragments[2].release();
                      })
                  .then(
                      () ->
                          stateMachineAssert
                              .hasSubscribedFlag()
                              .hasRequestN(1)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(payload::release)
                  .thenCancel()
                  .verifyLater();

          stepVerifier.verify();

          Assertions.assertThat(fragments).allMatch(bb -> bb.refCnt() == 0);

          return stepVerifier;
        });
  }

  /**
   * General StateMachine transition test. Fragmentation enabled In this test we check that the
   * given instance of RequestResponseMono: 1) subscribes 2) sends fragments frames on the first
   * request 3) terminates up on receiving the first signal (terminates on first next | error | next
   * over reassembly | complete)
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameFragmentsShouldBeSentOnSubscription(
      BiFunction<RequestResponseRequesterMono, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);
    final TestStreamManager activeStreams = TestStreamManager.client();

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            64,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestResponseRequesterMono,
            StepVerifier.create(requestResponseRequesterMono, 0)
                .expectSubscription()
                .then(stateMachineAssert::hasSubscribedFlagOnly)
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(activeStreams::assertNoActiveStreams)
                .thenRequest(1)
                .then(
                    () ->
                        stateMachineAssert
                            .hasSubscribedFlag()
                            .hasRequestN(1)
                            .hasFirstFrameSentFlag())
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestResponseRequesterMono)))
        .verify();

    // should not add anything to map
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.poll();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET_WITH_METADATA) // 64 - 6 (frame headers) - 3 (encoded metadata
        // length) - 3 frame length
        .hasMetadata(Arrays.copyOf(metadata, 52))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment2 = sender.poll();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET_WITH_METADATA) // 64 - 6 (frame headers) - 3 (encoded metadata
        // length) - 3 frame length
        .hasMetadata(Arrays.copyOfRange(metadata, 52, 65))
        .hasData(Arrays.copyOf(data, 39))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment3 = sender.poll();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET) // 64 - 6 (frame headers) - 3 frame length (no metadata - no length)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 39, 94))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment4 = sender.poll();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(35)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 94, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.poll())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();
    }
    Assertions.assertThat(sender).isEmpty();
    stateMachineAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General StateMachine transition test. Ensures that no fragment is sent if mono was cancelled
   * before any requests
   */
  @Test
  public void shouldBeNoOpsOnCancel() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    activeStreams.assertNoActiveStreams();
    stateMachineAssert.isUnsubscribed();

    StepVerifier.create(requestResponseRequesterMono, 0)
        .expectSubscription()
        .then(() -> stateMachineAssert.hasSubscribedFlagOnly())
        .then(() -> activeStreams.assertNoActiveStreams())
        .thenCancel()
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateMachineAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrate to
   * the terminated in case the given payload is an invalid one.
   */
  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<RequestResponseRequesterMono> monoConsumer) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestResponseRequesterMono);

    stateMachineAssert.isTerminated();
    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender).isEmpty();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestResponseRequesterMono>>
      shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestResponseRequesterMono ->
            Assertions.assertThatThrownBy(requestResponseRequesterMono::block)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrate to
   * the terminated in case the given payload was release in the middle of interaction.
   * Fragmentation is disabled
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhase() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("");

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestResponseRequesterMono, 0)
        .expectSubscription()
        .then(payload::release)
        .thenRequest(1)
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateMachineAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrate to
   * the terminated in case the given payload was release in the middle of interaction.
   * Fragmentation is enabled
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhaseWithFragmentation() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            64,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestResponseRequesterMono, 0)
        .expectSubscription()
        .then(payload::release)
        .thenRequest(1)
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateMachineAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrates
   * to the terminated in case the given payload is too big with disabled fragmentation
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestResponseRequesterMono> monoConsumer) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();

    final byte[] metadata = new byte[FRAME_LENGTH_MASK];
    final byte[] data = new byte[FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    activeStreams.assertNoActiveStreams();
    stateMachineAssert.isUnsubscribed();

    monoConsumer.accept(requestResponseRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender).isEmpty();
    stateMachineAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestResponseRequesterMono>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage(
                                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        requestResponseRequesterMono ->
            Assertions.assertThatThrownBy(requestResponseRequesterMono::block)
                .hasMessage(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                .isInstanceOf(IllegalArgumentException.class));
  }

  /**
   * Ensures that error check happens exactly before frame sent. This cases ensures that in case no
   * lease / other external errors appeared, the local subscriber received the same one. No frames
   * should be sent
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestResponseRequesterMono> monoConsumer) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client(new RuntimeException("test"));
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
        StateMachineAssert.assertThat(requestResponseRequesterMono);

    activeStreams.assertNoActiveStreams();
    stateMachineAssert.isUnsubscribed();

    monoConsumer.accept(requestResponseRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    stateMachineAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestResponseRequesterMono>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s, 0)
                .expectSubscription()
                .then(() -> StateMachineAssert.assertThat(s).hasSubscribedFlagOnly())
                .thenRequest(1)
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        requestResponseRequesterMono ->
            Assertions.assertThatThrownBy(requestResponseRequesterMono::block)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  /*
   * +--------------------------------+
   * |       Racing Test Cases        |
   * +--------------------------------+
   */

  /** Ensures single subscription happens in case of racing */
  @Test
  public void shouldSubscribeExactlyOnce1() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(
              allocator,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () -> {
                        AtomicReference<Throwable> atomicReference = new AtomicReference();
                        requestResponseRequesterMono.subscribe(null, atomicReference::set);
                        Throwable throwable = atomicReference.get();
                        if (throwable != null) {
                          throw Exceptions.propagate(throwable);
                        }
                      },
                      () -> {
                        AtomicReference<Throwable> atomicReference = new AtomicReference();
                        requestResponseRequesterMono.subscribe(null, atomicReference::set);
                        Throwable throwable = atomicReference.get();
                        if (throwable != null) {
                          throw Exceptions.propagate(throwable);
                        }
                      }))
          .matches(
              t -> {
                Assertions.assertThat(t)
                    .hasMessageContaining("RequestResponseMono allows only a single Subscriber");
                return true;
              });

      final ByteBuf frame = sender.poll();
      FrameAssert.assertThat(frame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();
    }

    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  /** Ensures single frame is sent only once racing between requests */
  @Test
  public void shouldSentRequestResponseFrameOnceInCaseOfRequestRacing() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(
              allocator,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseRequesterMono
              .doOnNext(Payload::release)
              .subscribeWith(AssertSubscriber.create(0));

      RaceTestUtils.race(() -> assertSubscriber.request(1), () -> assertSubscriber.request(1));

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      requestResponseRequesterMono.handlePayload(response);

      assertSubscriber.assertTerminated();

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
    allocator.assertHasNoLeaks();
  }

  /**
   * Ensures that no ByteBuf is leaked if reassembly is starting and cancel is happening at the same
   * time
   */
  @Test
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(
              allocator,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
          StateMachineAssert.assertThat(requestResponseRequesterMono);
      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseRequesterMono.subscribeWith(new AssertSubscriber<>(1));

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      int mtu = ThreadLocalRandom.current().nextInt(64, 256);
      Payload responsePayload = randomPayload();
      ArrayList<ByteBuf> fragments = prepareFragments(allocator, mtu, responsePayload);
      RaceTestUtils.race(
          requestResponseRequesterMono::cancel,
          () -> {
            int lastFragmentId = fragments.size() - 1;
            for (int j = 0; j < fragments.size(); j++) {
              ByteBuf frame = fragments.get(j);
              requestResponseRequesterMono.handleNext(frame, lastFragmentId != j, false);
              frame.release();
            }
          });

      if (assertSubscriber.isTerminated()) {
        Assertions.assertThat(assertSubscriber.values())
            .hasSize(1)
            .first()
            .matches(
                p -> {
                  Assertions.assertThat(p.sliceData())
                      .matches(bb -> ByteBufUtil.equals(bb, responsePayload.sliceData()));
                  Assertions.assertThat(p.hasMetadata()).isEqualTo(responsePayload.hasMetadata());
                  Assertions.assertThat(p.sliceMetadata())
                      .matches(bb -> ByteBufUtil.equals(bb, responsePayload.sliceMetadata()));
                  return true;
                });
      } else {
        final ByteBuf cancellationFrame = sender.poll();
        FrameAssert.assertThat(cancellationFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }

      Assertions.assertThat(responsePayload.release()).isTrue();
      Assertions.assertThat(responsePayload.refCnt()).isZero();
      Assertions.assertThat(payload.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();

      stateMachineAssert.isTerminated();
    }
    allocator.assertHasNoLeaks();
  }

  static Payload randomPayload() {
    boolean hasMetadata = ThreadLocalRandom.current().nextBoolean();
    byte[] randomMetadata;
    if (hasMetadata) {
      randomMetadata = new byte[ThreadLocalRandom.current().nextInt(0, 512)];
      ThreadLocalRandom.current().nextBytes(randomMetadata);
    } else {
      randomMetadata = null;
    }
    byte[] randomData = new byte[ThreadLocalRandom.current().nextInt(512, 1024)];
    ThreadLocalRandom.current().nextBytes(randomData);

    return ByteBufPayload.create(randomData, randomMetadata);
  }

  static ArrayList<ByteBuf> prepareFragments(
      LeaksTrackingByteBufAllocator allocator, int mtu, Payload payload) {
    boolean hasMetadata = payload.hasMetadata();
    ByteBuf data = payload.sliceData();
    ByteBuf metadata = payload.sliceMetadata();
    ArrayList<ByteBuf> fragments = new ArrayList<>();

    fragments.add(
        FragmentationUtils.encodeFirstFragment(
            allocator, mtu, FrameType.REQUEST_RESPONSE, 1, hasMetadata, metadata, data));

    while (metadata.isReadable() || data.isReadable()) {
      fragments.add(
          FragmentationUtils.encodeFollowsFragment(allocator, mtu, 1, false, metadata, data));
    }

    return fragments;
  }

  /**
   * Ensures that in case of racing between next element and cancel we will not have any memory
   * leaks
   */
  @Test
  public void shouldHaveNoLeaksOnNextAndCancelRacing() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(
              allocator,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      StepVerifier.create(requestResponseRequesterMono.doOnNext(Payload::release))
          .expectSubscription()
          .expectComplete()
          .verifyLater();

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      RaceTestUtils.race(
          requestResponseRequesterMono::cancel,
          () -> requestResponseRequesterMono.handlePayload(response));

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      final boolean isEmpty = sender.isEmpty();
      if (!isEmpty) {
        final ByteBuf cancellationFrame = sender.poll();
        FrameAssert.assertThat(cancellationFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }
      Assertions.assertThat(sender.isEmpty()).isTrue();

      StateMachineAssert.assertThat(requestResponseRequesterMono).isTerminated();
    }

    allocator.assertHasNoLeaks();
  }

  /**
   * Ensures that in case we have element reassembling and then it happens the remote sends
   * (errorFrame) and downstream subscriber sends cancel() and we have racing between onError and
   * cancel we will not have any memory leaks
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void shouldHaveNoUnexpectedErrorDuringOnErrorAndCancelRacing(boolean withReassembly) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final ArrayList<Throwable> droppedErrors = new ArrayList<>();
    Hooks.onErrorDropped(droppedErrors::add);
    try {
      for (int i = 0; i < 10000; i++) {
        final TestStreamManager activeStreams = TestStreamManager.client();
        final Payload payload = ByteBufPayload.create("testData", "testMetadata");

        final RequestResponseRequesterMono requestResponseRequesterMono =
            new RequestResponseRequesterMono(
                allocator,
                payload,
                0,
                FRAME_LENGTH_MASK,
                Integer.MAX_VALUE,
                activeStreams,
                sender,
                PayloadDecoder.ZERO_COPY);

        final StateMachineAssert<RequestResponseRequesterMono> stateMachineAssert =
            StateMachineAssert.assertThat(requestResponseRequesterMono);

        stateMachineAssert.isUnsubscribed();
        final AssertSubscriber<Payload> assertSubscriber =
            requestResponseRequesterMono.subscribeWith(AssertSubscriber.create(0));

        stateMachineAssert.hasSubscribedFlagOnly();

        assertSubscriber.request(1);

        stateMachineAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag();

        final ByteBuf sentFrame = sender.poll();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .hasPayloadSize(
                "testData".getBytes(CharsetUtil.UTF_8).length
                    + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
            .hasMetadata("testMetadata")
            .hasData("testData")
            .hasNoFragmentsFollow()
            .typeOf(FrameType.REQUEST_RESPONSE)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();

        if (withReassembly) {
          final ByteBuf fragmentBuf = allocator.buffer().writeBytes(new byte[] {1, 2, 3});
          requestResponseRequesterMono.handleNext(fragmentBuf, true, false);
          // mimic frameHandler behaviour
          fragmentBuf.release();
        }

        final RuntimeException testException = new RuntimeException("test");
        RaceTestUtils.race(
            requestResponseRequesterMono::cancel,
            () -> requestResponseRequesterMono.handleError(testException));

        Assertions.assertThat(payload.refCnt()).isZero();

        activeStreams.assertNoActiveStreams();
        stateMachineAssert.isTerminated();

        final boolean isEmpty = sender.isEmpty();
        if (!isEmpty) {
          final ByteBuf cancellationFrame = sender.poll();
          FrameAssert.assertThat(cancellationFrame)
              .isNotNull()
              .typeOf(FrameType.CANCEL)
              .hasClientSideStreamId()
              .hasStreamId(1)
              .hasNoLeaks();

          Assertions.assertThat(droppedErrors).containsExactly(testException);
        } else {
          assertSubscriber.assertTerminated().assertErrorMessage("test");
        }
        Assertions.assertThat(sender.isEmpty()).isTrue();

        stateMachineAssert.isTerminated();
        droppedErrors.clear();
      }
      allocator.assertHasNoLeaks();
    } finally {
      Hooks.resetOnErrorDropped();
    }
  }

  /**
   * Ensures that in case of racing between first request and cancel does not going to introduce
   * leaks. <br>
   * <br>
   *
   * <p>Please note, first request may or may not happen so in case it happened before cancellation
   * signal we have to observe
   *
   * <ul>
   *   <li>RequestResponseFrame
   *   <li>CancellationFrame
   * </ul>
   *
   * <p>exactly in that order
   *
   * <p>Ensures full serialization of outgoing signal (frames)
   */
  @Test
  public void shouldBeConsistentInCaseOfRacingOfCancellationAndRequest() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(
              allocator,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseRequesterMono.subscribeWith(new AssertSubscriber<>(0));

      RaceTestUtils.race(() -> assertSubscriber.cancel(), () -> assertSubscriber.request(1));

      if (!sender.isEmpty()) {
        final ByteBuf sentFrame = sender.poll();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_RESPONSE)
            .hasPayloadSize(
                "testData".getBytes(CharsetUtil.UTF_8).length
                    + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
            .hasMetadata("testMetadata")
            .hasData("testData")
            .hasNoFragmentsFollow()
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();

        final ByteBuf cancelFrame = sender.poll();
        FrameAssert.assertThat(cancelFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }

      Assertions.assertThat(payload.refCnt()).isZero();

      StateMachineAssert.assertThat(requestResponseRequesterMono).isTerminated();

      requestResponseRequesterMono.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
    allocator.assertHasNoLeaks();
  }

  /** Ensures that CancelFrame is sent exactly once in case of racing between cancel() methods */
  @Test
  public void shouldSentCancelFrameExactlyOnce() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(
              allocator,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseRequesterMono.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasStreamId(1)
          .hasNoLeaks();

      RaceTestUtils.race(
          requestResponseRequesterMono::cancel, requestResponseRequesterMono::cancel);

      final ByteBuf cancelFrame = sender.poll();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      StateMachineAssert.assertThat(requestResponseRequesterMono).isTerminated();

      requestResponseRequesterMono.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestResponseRequesterMono.handleComplete();
      assertSubscriber.assertNotTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
    allocator.assertHasNoLeaks();
  }

  @Test
  public void checkName() {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(
            allocator,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(Scannable.from(requestResponseRequesterMono).name())
        .isEqualTo("source(RequestResponseMono)");
    allocator.assertHasNoLeaks();
  }
}
