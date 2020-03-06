package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N;
import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class RequestStreamRequesterFluxTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /*
   * +-------------------------------+
   * |      General Test Cases       |
   * +-------------------------------+
   */

  /**
   * State Machine check. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(1) -> REQUESTED(0)
   * REQUESTED(0) -> REQUESTED(1) -> REQUESTED(0)
   * REQUESTED(0) -> REQUESTED(MAX)
   * REQUESTED(MAX) -> REQUESTED(MAX) && REASSEMBLY (extra flag enabled which indicates
   * reassembly)
   * REQUESTED(MAX) && REASSEMBLY -> TERMINATED
   * </pre>
   */
  @Test
  public void requestNFrameShouldBeSentOnSubscriptionAndThenSeparately() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final TestStreamManager activeStreams = TestStreamManager.client();

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestStreamRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(payload.refCnt()).isOne();
    activeStreams.assertNoActiveStreams();
    // state machine check
    stateMachineAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(1);

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertHasStream(1, requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag();

    final ByteBuf frame = sender.poll();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(sender.isEmpty()).isTrue();

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();

    assertSubscriber.request(1);
    final ByteBuf requestNFrame = sender.poll();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_N)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(sender.isEmpty()).isTrue();

    Assertions.assertThat(requestNFrame.release()).isTrue();
    Assertions.assertThat(requestNFrame.refCnt()).isZero();

    // state machine check. Request N Frame should sent so request field should be 0
    // state machine check
    stateMachineAssert.hasSubscribedFlag().hasRequestN(2).hasFirstFrameSentFlag();

    assertSubscriber.request(Long.MAX_VALUE);
    final ByteBuf requestMaxNFrame = sender.poll();
    FrameAssert.assertThat(requestMaxNFrame)
        .isNotNull()
        .hasRequestN(Integer.MAX_VALUE)
        .typeOf(FrameType.REQUEST_N)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(sender.isEmpty()).isTrue();

    Assertions.assertThat(requestMaxNFrame.release()).isTrue();
    Assertions.assertThat(requestMaxNFrame.refCnt()).isZero();

    // state machine check

    // state machine check
    stateMachineAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    assertSubscriber.request(6);
    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check
    stateMachineAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    // no intent to check reassembly correctness here, just to ensure that state is
    // correctly formed
    requestStreamRequesterFlux.handleNext(Unpooled.EMPTY_BUFFER, true, false);

    // state machine check
    stateMachineAssert
        .hasSubscribedFlag()
        .hasRequestN(Integer.MAX_VALUE)
        .hasFirstFrameSentFlag()
        .hasReassemblingFlag();

    requestStreamRequesterFlux.handlePayload(EmptyPayload.INSTANCE);

    requestStreamRequesterFlux.handleComplete();
    assertSubscriber.assertValues(EmptyPayload.INSTANCE).assertComplete();

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check
    stateMachineAssert.isTerminated();
  }

  /**
   * State Machine check. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(MAX)
   * REQUESTED(MAX) -> TERMINATED
   * </pre>
   */
  @Test
  public void requestNFrameShouldBeSentExactlyOnceIfItIsMaxAllowed() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final TestStreamManager activeStreams = TestStreamManager.client();

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestStreamRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(payload.refCnt()).isOne();
    activeStreams.assertNoActiveStreams();

    // state machine check
    stateMachineAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(Long.MAX_VALUE / 2 + 1);

    // state machine check

    stateMachineAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertHasStream(1, requestStreamRequesterFlux);

    final ByteBuf frame = sender.poll();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .hasRequestN(Integer.MAX_VALUE)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(sender.isEmpty()).isTrue();

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();

    assertSubscriber.request(1);
    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check

    stateMachineAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    requestStreamRequesterFlux.handlePayload(EmptyPayload.INSTANCE);
    requestStreamRequesterFlux.handleComplete();

    assertSubscriber.assertValues(EmptyPayload.INSTANCE).assertComplete();

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertNoActiveStreams();
    // state machine check
    stateMachineAssert.isTerminated();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  /**
   * State Machine check. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(1) -> REQUESTED(0)
   * </pre>
   *
   * And then for the following cases:
   *
   * <pre>
   * [0]: REQUESTED(0) -> REQUESTED(MAX) (with onNext and few extra request(1) which should not
   * affect state anyhow and should not sent any extra frames)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [1]: REQUESTED(0) -> REQUESTED(MAX) (with onComplete rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [2]: REQUESTED(0) -> REQUESTED(MAX) (with onError rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [3]: REQUESTED(0) -> REASSEMBLY
   *      REASSEMBLY -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [4]: REQUESTED(0) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> TERMINATED (because of cancel() invocation)
   * </pre>
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnFirstRequestResponses")
  public void frameShouldBeSentOnFirstRequest(
      BiFunction<RequestStreamRequesterFlux, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final TestStreamManager activeStreams = TestStreamManager.client();

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check

    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestStreamRequesterFlux,
            StepVerifier.create(requestStreamRequesterFlux, 0)
                .expectSubscription()
                .then(
                    () ->
                        // state machine check
                        stateMachineAssert.hasSubscribedFlagOnly())
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> activeStreams.assertNoActiveStreams())
                .thenRequest(1)
                .then(
                    () ->
                        // state machine check
                        stateMachineAssert
                            .hasSubscribedFlag()
                            .hasRequestN(1)
                            .hasFirstFrameSentFlag())
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestStreamRequesterFlux)))
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
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();

    final ByteBuf requestNFrame = sender.poll();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .typeOf(FrameType.REQUEST_N)
        .hasRequestN(Integer.MAX_VALUE)
        .hasClientSideStreamId()
        .hasStreamId(1);
    Assertions.assertThat(requestNFrame.release()).isTrue();
    Assertions.assertThat(requestNFrame.refCnt()).isZero();

    if (!sender.isEmpty()) {
      final ByteBuf cancelFrame = sender.poll();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);
      Assertions.assertThat(cancelFrame.release()).isTrue();
      Assertions.assertThat(cancelFrame.refCnt()).isZero();
    }
    // state machine check
    stateMachineAssert.isTerminated();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<BiFunction<RequestStreamRequesterFlux, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnFirstRequestResponses() {
    return Stream.of(
        (rsf, sv) ->
            sv.then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(Long.MAX_VALUE)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(rsf::handleComplete)
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf).isTerminated())
                .expectComplete(),
        (rsf, sv) ->
            sv.then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(Long.MAX_VALUE)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(rsf::handleComplete)
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf).isTerminated())
                .expectComplete(),
        (rsf, sv) ->
            sv.then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(Long.MAX_VALUE)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(() -> rsf.handleError(new ApplicationErrorException("test")))
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(rsf).isTerminated())
                .thenRequest(1L)
                .thenRequest(1L)
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        (rsf, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          final Payload payload2 = ByteBufPayload.create(data, metadata);

          return sv.then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFirstFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            FrameType.NEXT,
                            1,
                            payload.hasMetadata(),
                            payload.metadata(),
                            payload.data());
                    rsf.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(1)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rsf.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(1)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rsf.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(1)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .thenRequest(Long.MAX_VALUE)
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rsf.handleNext(followingFrame, false, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasNoReassemblingFlag())
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    Assertions.assertThat(p.release()).isTrue();
                    Assertions.assertThat(p.refCnt()).isZero();
                  })
              .then(payload::release)
              .then(() -> rsf.handlePayload(payload2))
              .thenRequest(1)
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag())
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    Assertions.assertThat(p.release()).isTrue();
                    Assertions.assertThat(p.refCnt()).isZero();
                  })
              .thenRequest(1)
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag())
              .then(rsf::handleComplete)
              .then(
                  () ->
                      // state machine check
                      StateMachineAssert.assertThat(rsf).isTerminated())
              .expectComplete();
        },
        (rsf, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload0 = ByteBufPayload.create(data, metadata);
          final Payload payload = ByteBufPayload.create(data, metadata);

          ByteBuf[] fragments =
              new ByteBuf[] {
                FragmentationUtils.encodeFirstFragment(
                    ByteBufAllocator.DEFAULT,
                    64,
                    FrameType.NEXT,
                    1,
                    payload.hasMetadata(),
                    payload.metadata(),
                    payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    ByteBufAllocator.DEFAULT, 64, 1, false, payload.metadata(), payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    ByteBufAllocator.DEFAULT, 64, 1, false, payload.metadata(), payload.data())
              };

          final StepVerifier stepVerifier =
              sv.then(() -> rsf.handlePayload(payload0))
                  .assertNext(
                      p -> {
                        Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                        Assertions.assertThat(p.metadata())
                            .isEqualTo(Unpooled.wrappedBuffer(metadata));
                        Assertions.assertThat(p.release()).isTrue();
                        Assertions.assertThat(p.refCnt()).isZero();
                      })
                  .thenRequest(Long.MAX_VALUE)
                  .then(
                      () ->
                          // state machine check
                          StateMachineAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasNoReassemblingFlag())
                  .then(
                      () -> {
                        rsf.handleNext(fragments[0], true, false);
                        fragments[0].release();
                      })
                  .then(
                      () ->
                          // state machine check
                          StateMachineAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rsf.handleNext(fragments[1], true, false);
                        fragments[1].release();
                      })
                  .then(
                      () ->
                          // state machine check
                          StateMachineAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rsf.handleNext(fragments[2], true, false);
                        fragments[2].release();
                      })
                  .then(
                      () ->
                          // state machine check
                          StateMachineAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .thenRequest(1)
                  .then(
                      () ->
                          // state machine check
                          StateMachineAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .thenRequest(1)
                  .then(
                      () ->
                          // state machine check
                          StateMachineAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(payload::release)
                  .thenCancel()
                  .verifyLater();

          stepVerifier.verify();
          // state machine check
          StateMachineAssert.assertThat(rsf).isTerminated();

          Assertions.assertThat(fragments).allMatch(bb -> bb.refCnt() == 0);

          return stepVerifier;
        });
  }

  /**
   * State Machine check with fragmentation of the first payload. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(1) -> REQUESTED(0)
   * </pre>
   *
   * And then for the following cases:
   *
   * <pre>
   * [0]: REQUESTED(0) -> REQUESTED(MAX) (with onNext and few extra request(1) which should not
   * affect state anyhow and should not sent any extra frames)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [1]: REQUESTED(0) -> REQUESTED(MAX) (with onComplete rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [2]: REQUESTED(0) -> REQUESTED(MAX) (with onError rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [3]: REQUESTED(0) -> REASSEMBLY
   *      REASSEMBLY -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [4]: REQUESTED(0) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> TERMINATED (because of cancel() invocation)
   * </pre>
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnFirstRequestResponses")
  public void frameFragmentsShouldBeSentOnFirstRequest(
      BiFunction<RequestStreamRequesterFlux, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);
    final TestStreamManager activeStreams = TestStreamManager.client();

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            64,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestStreamRequesterFlux,
            StepVerifier.create(requestStreamRequesterFlux, 0)
                .expectSubscription()
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> activeStreams.assertNoActiveStreams())
                .thenRequest(1)
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestStreamRequesterFlux)))
        .verify();

    // should not add anything to map
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.poll();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(64 - FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N)
        // InitialRequestN size
        .hasMetadata(Arrays.copyOf(metadata, 64 - FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment1.release()).isTrue();
    Assertions.assertThat(frameFragment1.refCnt()).isZero();

    final ByteBuf frameFragment2 = sender.poll();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(64 - FRAME_OFFSET_WITH_METADATA)
        .hasMetadata(
            Arrays.copyOfRange(metadata, 64 - FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N, 65))
        .hasData(Arrays.copyOf(data, 35))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment2.release()).isTrue();
    Assertions.assertThat(frameFragment2.refCnt()).isZero();

    final ByteBuf frameFragment3 = sender.poll();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(64 - FRAME_OFFSET)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 35, 35 + 55))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment3.release()).isTrue();
    Assertions.assertThat(frameFragment3.refCnt()).isZero();

    final ByteBuf frameFragment4 = sender.poll();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(39)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 90, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment4.release()).isTrue();
    Assertions.assertThat(frameFragment4.refCnt()).isZero();

    final ByteBuf requestNFrame = sender.poll();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .typeOf(FrameType.REQUEST_N)
        .hasRequestN(Integer.MAX_VALUE)
        .hasClientSideStreamId()
        .hasStreamId(1);
    Assertions.assertThat(requestNFrame.release()).isTrue();
    Assertions.assertThat(requestNFrame.refCnt()).isZero();

    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.poll())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);
    }
    Assertions.assertThat(sender).isEmpty();
    // state machine check
    stateMachineAssert.isTerminated();
  }

  /**
   * Case which ensures that if Payload has incorrect refCnt, the flux ends up with an appropriate
   * error
   */
  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<RequestStreamRequesterFlux> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestStreamRequesterFlux);

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender).isEmpty();
    // state machine check
    stateMachineAssert.isTerminated();
  }

  static Stream<Consumer<RequestStreamRequesterFlux>>
      shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestStreamRequesterFlux ->
            Assertions.assertThatThrownBy(requestStreamRequesterFlux::blockLast)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  /**
   * Ensures that if Payload is release right after the subscription, the first request will exponse
   * the error immediatelly and no frame will be sent to the remote party
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhase() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("");

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestStreamRequesterFlux, 0)
        .expectSubscription()
        .then(
            () ->
                // state machine check
                stateMachineAssert.hasSubscribedFlagOnly())
        .then(payload::release)
        .thenRequest(1)
        .then(
            () ->
                // state machine check
                stateMachineAssert.isTerminated())
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender).isEmpty();

    // state machine check
    stateMachineAssert.isTerminated();
  }

  /**
   * Ensures that if Payload is release right after the subscription, the first request will expose
   * the error immediately and no frame will be sent to the remote party
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhaseWithFragmentation() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            64,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestStreamRequesterFlux, 0)
        .expectSubscription()
        .then(
            () ->
                // state machine check
                stateMachineAssert.hasSubscribedFlagOnly())
        .then(payload::release)
        .thenRequest(1)
        .then(
            () ->
                // state machine check
                stateMachineAssert.isTerminated())
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender).isEmpty();
    // state machine check
    stateMachineAssert.isTerminated();
  }

  /**
   * Ensures that if the given payload is exits 16mb size with disabled fragmentation, than the
   * appropriate validation happens and a corresponding error will be propagagted to the subscriber
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestStreamRequesterFlux> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();

    final byte[] metadata = new byte[FRAME_LENGTH_MASK];
    final byte[] data = new byte[FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestStreamRequesterFlux);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender).isEmpty();
    // state machine check
    stateMachineAssert.isTerminated();
  }

  static Stream<Consumer<RequestStreamRequesterFlux>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s, 0)
                .expectSubscription()
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(s).isTerminated())
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage(
                                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        requestStreamRequesterFlux ->
            Assertions.assertThatThrownBy(requestStreamRequesterFlux::blockLast)
                .hasMessage(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                .isInstanceOf(IllegalArgumentException.class));
  }

  /**
   * Ensures that the interactions check and respect rsocket availability (such as leasing) and
   * propagate an error to the final subscriber. No frame should be sent. Check should happens
   * exactly on the first request.
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestStreamRequesterFlux> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client(new RuntimeException("test"));
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);
    final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
        StateMachineAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateMachineAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestStreamRequesterFlux);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
  }

  static Stream<Consumer<RequestStreamRequesterFlux>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s, 0)
                .expectSubscription()
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(s).hasSubscribedFlagOnly())
                .thenRequest(1)
                .then(
                    () ->
                        // state machine check
                        StateMachineAssert.assertThat(s).isTerminated())
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        requestStreamRequesterFlux ->
            Assertions.assertThatThrownBy(requestStreamRequesterFlux::blockLast)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  /*
   * +--------------------------------+
   * |       Racing Test Cases        |
   * +--------------------------------+
   */

  @Test
  public void shouldSubscribeExactlyOnce1() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 100000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
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
                        AtomicReference<Throwable> atomicReference = new AtomicReference<>();
                        requestStreamRequesterFlux.subscribe(null, atomicReference::set);
                        Throwable throwable = atomicReference.get();
                        if (throwable != null) {
                          throw Exceptions.propagate(throwable);
                        }
                      },
                      () -> {
                        AtomicReference<Throwable> atomicReference = new AtomicReference<>();
                        requestStreamRequesterFlux.subscribe(null, atomicReference::set);
                        Throwable throwable = atomicReference.get();
                        if (throwable != null) {
                          throw Exceptions.propagate(throwable);
                        }
                      }))
          .matches(
              t -> {
                Assertions.assertThat(t)
                    .hasMessageContaining("RequestStreamFlux allows only a single Subscriber");
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
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(frame.release()).isTrue();
      Assertions.assertThat(frame.refCnt()).isZero();
    }

    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldBeNoOpsOnCancel() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    StepVerifier.create(requestStreamRequesterFlux, 0)
        .expectSubscription()
        .then(() -> activeStreams.assertNoActiveStreams())
        .thenCancel()
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing() {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      ByteBuf frame = Unpooled.wrappedBuffer("test".getBytes(CharsetUtil.UTF_8));

      StepVerifier.create(requestStreamRequesterFlux)
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
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(
          requestStreamRequesterFlux::cancel,
          () -> {
            requestStreamRequesterFlux.handleNext(frame, true, false);
            frame.release();
          });

      final ByteBuf cancellationFrame = sender.poll();
      FrameAssert.assertThat(cancellationFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(frame.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void shouldHaveNoLeaksOnNextAndCancelRacing() {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      StepVerifier.create(requestStreamRequesterFlux.doOnNext(Payload::release))
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
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(
          requestStreamRequesterFlux::cancel,
          () -> requestStreamRequesterFlux.handlePayload(response));

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
            .hasStreamId(1);
      }
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldSentRequestStreamFrameOnceInCaseOfRequestRacing(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux
              .doOnNext(Payload::release)
              .subscribeWith(AssertSubscriber.create(0));

      RaceTestUtils.race(
          cases.apply(requestStreamRequesterFlux), cases.apply(requestStreamRequesterFlux));

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasRequestN(expectedN)
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();

      if (realRequestN < Integer.MAX_VALUE) {
        final ByteBuf requestNFrame = sender.poll();
        FrameAssert.assertThat(requestNFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(expectedN)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(requestNFrame.release()).isTrue();
        Assertions.assertThat(requestNFrame.refCnt()).isZero();

        stateMachineAssert.hasSubscribedFlag().hasRequestN(expectedN * 2).hasFirstFrameSentFlag();
      } else {
        stateMachineAssert.hasFirstFrameSentFlag().hasRequestN(expectedN).hasFirstFrameSentFlag();
      }

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber.assertTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRequestRacing(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux
              .doOnNext(Payload::release)
              .subscribeWith(AssertSubscriber.create(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();

      RaceTestUtils.race(
          cases.apply(requestStreamRequesterFlux), cases.apply(requestStreamRequesterFlux));

      final ByteBuf requestNFrame1 = sender.poll();
      FrameAssert.assertThat(requestNFrame1)
          .isNotNull()
          .typeOf(FrameType.REQUEST_N)
          .hasRequestN(expectedN)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(requestNFrame1.release()).isTrue();
      Assertions.assertThat(requestNFrame1.refCnt()).isZero();

      if (realRequestN < Integer.MAX_VALUE) {
        final ByteBuf requestNFrame2 = sender.poll();
        FrameAssert.assertThat(requestNFrame2)
            .isNotNull()
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(expectedN)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(requestNFrame2.release()).isTrue();
        Assertions.assertThat(requestNFrame2.refCnt()).isZero();

        stateMachineAssert
            .hasSubscribedFlag()
            .hasRequestN(expectedN * 2 + 1)
            .hasFirstFrameSentFlag();
      } else {
        stateMachineAssert
            .hasSubscribedFlag()
            .hasRequestN(Integer.MAX_VALUE)
            .hasFirstFrameSentFlag();
      }

      stateMachineAssert.hasNoReassemblingFlag();

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber.assertTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRequestRacingDuringReassembly(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux
              .doOnNext(Payload::release)
              .subscribeWith(AssertSubscriber.create(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();

      requestStreamRequesterFlux.handleNext(Unpooled.EMPTY_BUFFER, true, false);

      RaceTestUtils.race(
          cases.apply(requestStreamRequesterFlux), cases.apply(requestStreamRequesterFlux));

      final ByteBuf requestNFrame1 = sender.poll();
      FrameAssert.assertThat(requestNFrame1)
          .isNotNull()
          .typeOf(FrameType.REQUEST_N)
          .hasRequestN(expectedN)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(requestNFrame1.release()).isTrue();
      Assertions.assertThat(requestNFrame1.refCnt()).isZero();

      if (realRequestN < Integer.MAX_VALUE) {
        final ByteBuf requestNFrame2 = sender.poll();
        FrameAssert.assertThat(requestNFrame2)
            .isNotNull()
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(expectedN)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(requestNFrame2.release()).isTrue();
        Assertions.assertThat(requestNFrame2.refCnt()).isZero();
        stateMachineAssert
            .hasSubscribedFlag()
            .hasRequestN(expectedN * 2 + 1)
            .hasFirstFrameSentFlag();
      } else {
        stateMachineAssert
            .hasSubscribedFlag()
            .hasRequestN(Integer.MAX_VALUE)
            .hasFirstFrameSentFlag();
      }

      stateMachineAssert.hasReassemblingFlag();

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber.assertTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRacingOfReassemblyAndRequest(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux
              .doOnNext(Payload::release)
              .subscribeWith(AssertSubscriber.create(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();

      RaceTestUtils.race(
          () -> requestStreamRequesterFlux.handleNext(Unpooled.EMPTY_BUFFER, true, false),
          cases.apply(requestStreamRequesterFlux));

      final ByteBuf requestNFrame1 = sender.poll();
      FrameAssert.assertThat(requestNFrame1)
          .isNotNull()
          .typeOf(FrameType.REQUEST_N)
          .hasRequestN(expectedN)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(requestNFrame1.release()).isTrue();
      Assertions.assertThat(requestNFrame1.refCnt()).isZero();

      stateMachineAssert.hasReassemblingFlag();

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber.assertTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRacingOfCancellationAndRequest(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux.subscribeWith(new AssertSubscriber<>(0));

      RaceTestUtils.race(
          requestStreamRequesterFlux::cancel, cases.apply(requestStreamRequesterFlux));

      if (!sender.isEmpty()) {
        final ByteBuf sentFrame = sender.poll();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .hasNoFragmentsFollow()
            .hasRequestN(expectedN)
            .typeOf(FrameType.REQUEST_STREAM)
            .hasClientSideStreamId()
            .hasPayloadSize(
                "testData".getBytes(CharsetUtil.UTF_8).length
                    + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
            .hasMetadata("testMetadata")
            .hasData("testData")
            .hasStreamId(1);

        Assertions.assertThat(sentFrame.release()).isTrue();
        Assertions.assertThat(sentFrame.refCnt()).isZero();

        final ByteBuf cancelFrame = sender.poll();
        FrameAssert.assertThat(cancelFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(cancelFrame.release()).isTrue();
        Assertions.assertThat(cancelFrame.refCnt()).isZero();
      }

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      stateMachineAssert.isTerminated();

      requestStreamRequesterFlux.handlePayload(response);
      requestStreamRequesterFlux.handleComplete();

      assertSubscriber.values().forEach(ReferenceCounted::release);
      assertSubscriber.assertNotTerminated();

      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRacingOfOnCompleteAndRequest(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .hasRequestN(1)
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(
          requestStreamRequesterFlux::handleComplete, cases.apply(requestStreamRequesterFlux));

      if (!sender.isEmpty()) {
        final ByteBuf requestNFrame = sender.poll();
        FrameAssert.assertThat(requestNFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(expectedN)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(requestNFrame.release()).isTrue();
        Assertions.assertThat(requestNFrame.refCnt()).isZero();
      }

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      stateMachineAssert.isTerminated();

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber.assertTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRacingOfOnErrorAndRequest(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .hasRequestN(1)
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RuntimeException exception = new RuntimeException("test");
      RaceTestUtils.race(
          () -> requestStreamRequesterFlux.handleError(exception),
          cases.apply(requestStreamRequesterFlux));

      if (!sender.isEmpty()) {
        final ByteBuf requestNFrame = sender.poll();
        FrameAssert.assertThat(requestNFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(expectedN)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(requestNFrame.release()).isTrue();
        Assertions.assertThat(requestNFrame.refCnt()).isZero();
      }

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      stateMachineAssert.isTerminated();

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber
          .assertTerminated()
          .assertError(RuntimeException.class)
          .assertErrorMessage("test");

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void shouldBeConsistentInCaseOfRacingOfOnErrorAndCancelDuringReassembly(
      Function<RequestStreamRequesterFlux, Runnable> cases, long realRequestN, int expectedN) {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .hasRequestN(1)
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      //      FragmentationUtils.encodeFirstFragment()

      requestStreamRequesterFlux.handleNext(Unpooled.EMPTY_BUFFER, true, false);
      stateMachineAssert.hasReassemblingFlag();

      RuntimeException exception = new RuntimeException("test");
      RaceTestUtils.race(
          () -> requestStreamRequesterFlux.handleError(exception),
          cases.apply(requestStreamRequesterFlux));

      if (!sender.isEmpty()) {
        final ByteBuf requestNFrame = sender.poll();
        FrameAssert.assertThat(requestNFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(expectedN)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(requestNFrame.release()).isTrue();
        Assertions.assertThat(requestNFrame.refCnt()).isZero();
      }

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      stateMachineAssert.isTerminated();

      requestStreamRequesterFlux.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestStreamRequesterFlux.handleComplete();
      assertSubscriber
          .assertTerminated()
          .assertError(RuntimeException.class)
          .assertErrorMessage("test");

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void shouldSentCancelFrameExactlyOnce() {
    for (int i = 0; i < 10000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamRequesterFlux requestStreamRequesterFlux =
          new RequestStreamRequesterFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);
      final StateMachineAssert<RequestStreamRequesterFlux> stateMachineAssert =
          StateMachineAssert.assertThat(requestStreamRequesterFlux);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestStreamRequesterFlux.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .hasRequestN(1)
          .typeOf(FrameType.REQUEST_STREAM)
          .hasClientSideStreamId()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(requestStreamRequesterFlux::cancel, requestStreamRequesterFlux::cancel);

      final ByteBuf cancelFrame = sender.poll();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(cancelFrame.release()).isTrue();
      Assertions.assertThat(cancelFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      stateMachineAssert.isTerminated();
      requestStreamRequesterFlux.handlePayload(response);
      requestStreamRequesterFlux.handleComplete();

      assertSubscriber.values().forEach(ReferenceCounted::release);
      assertSubscriber.assertNotTerminated();

      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  public static Stream<Arguments> racingCases() {
    return Stream.of(
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux -> () -> requestStreamRequesterFlux.request(1),
            1L,
            1),
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux ->
                    () -> requestStreamRequesterFlux.request(Long.MAX_VALUE),
            Long.MAX_VALUE,
            Integer.MAX_VALUE),
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux ->
                    () -> requestStreamRequesterFlux.request(Long.MAX_VALUE / 2 + 1),
            Long.MAX_VALUE / 2 + 1,
            Integer.MAX_VALUE),
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux ->
                    () -> requestStreamRequesterFlux.request(Long.MAX_VALUE / 2),
            Long.MAX_VALUE / 2,
            Integer.MAX_VALUE),
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux ->
                    () -> requestStreamRequesterFlux.request(Long.MAX_VALUE / 4 + 1),
            Long.MAX_VALUE / 4 + 1,
            Integer.MAX_VALUE),
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux ->
                    () -> requestStreamRequesterFlux.request(Long.MAX_VALUE / 4),
            Long.MAX_VALUE / 4,
            Integer.MAX_VALUE),
        Arguments.of(
            (Function<RequestStreamRequesterFlux, Runnable>)
                requestStreamRequesterFlux ->
                    () -> requestStreamRequesterFlux.request(Integer.MAX_VALUE / 2),
            Integer.MAX_VALUE / 2,
            Integer.MAX_VALUE / 2));
  }

  @Test
  public void checkName() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final TestStreamManager activeStreams = TestStreamManager.client();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(Scannable.from(requestStreamRequesterFlux).name())
        .isEqualTo("source(RequestStreamFlux)");
  }
}
