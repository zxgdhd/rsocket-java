package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameType.COMPLETE;
import static io.rsocket.frame.FrameType.METADATA_PUSH;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_N;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;
import static io.rsocket.frame.FrameType.REQUEST_STREAM;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.CharsetUtil;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

@SuppressWarnings("ALL")
public class RequesterOperatorsRacingTest {

  static final String DATA_CONTENT = "testData";
  static final String METADATA_CONTENT = "testMetadata";

  interface Scenario {
    FrameType requestType();

    Publisher<?> requestOperator(
        ByteBufAllocator allocator,
        Supplier<Payload> payloadsSupplier,
        UnboundedProcessor<ByteBuf> sender,
        StreamManager streamManager);
  }

  static Stream<Scenario> scenarios() {
    return Stream.of(
        new Scenario() {
          @Override
          public FrameType requestType() {
            return METADATA_PUSH;
          }

          @Override
          public Publisher<?> requestOperator(
              ByteBufAllocator allocator,
              Supplier<Payload> payloadsSupplier,
              UnboundedProcessor<ByteBuf> sender,
              StreamManager streamManager) {
            return new MetadataPushRequesterMono(
                allocator, payloadsSupplier.get(), FRAME_LENGTH_MASK, sender);
          }

          @Override
          public String toString() {
            return MetadataPushRequesterMono.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_FNF;
          }

          @Override
          public Publisher<?> requestOperator(
              ByteBufAllocator allocator,
              Supplier<Payload> payloadsSupplier,
              UnboundedProcessor<ByteBuf> sender,
              StreamManager streamManager) {
            return new FireAndForgetRequesterMono(
                allocator, payloadsSupplier.get(), 0, FRAME_LENGTH_MASK, streamManager, sender);
          }

          @Override
          public String toString() {
            return FireAndForgetRequesterMono.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_RESPONSE;
          }

          @Override
          public Publisher<?> requestOperator(
              ByteBufAllocator allocator,
              Supplier<Payload> payloadsSupplier,
              UnboundedProcessor<ByteBuf> sender,
              StreamManager streamManager) {
            return new RequestResponseRequesterMono(
                allocator,
                payloadsSupplier.get(),
                0,
                FRAME_LENGTH_MASK,
                Integer.MAX_VALUE,
                streamManager,
                sender,
                PayloadDecoder.ZERO_COPY);
          }

          @Override
          public String toString() {
            return RequestResponseRequesterMono.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_STREAM;
          }

          @Override
          public Publisher<?> requestOperator(
              ByteBufAllocator allocator,
              Supplier<Payload> payloadsSupplier,
              UnboundedProcessor<ByteBuf> sender,
              StreamManager streamManager) {
            return new RequestStreamRequesterFlux(
                allocator,
                payloadsSupplier.get(),
                0,
                FRAME_LENGTH_MASK,
                Integer.MAX_VALUE,
                streamManager,
                sender,
                PayloadDecoder.ZERO_COPY);
          }

          @Override
          public String toString() {
            return RequestStreamRequesterFlux.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_CHANNEL;
          }

          @Override
          public Publisher<?> requestOperator(
              ByteBufAllocator allocator,
              Supplier<Payload> payloadsSupplier,
              UnboundedProcessor<ByteBuf> sender,
              StreamManager streamManager) {
            return new RequestChannelRequesterFlux(
                allocator,
                Flux.generate(s -> s.next(payloadsSupplier.get())),
                0,
                FRAME_LENGTH_MASK,
                Integer.MAX_VALUE,
                streamManager,
                sender,
                PayloadDecoder.ZERO_COPY);
          }

          @Override
          public String toString() {
            return RequestChannelRequesterFlux.class.getSimpleName();
          }
        });
  }
  /*
   * +--------------------------------+
   * |       Racing Test Cases        |
   * +--------------------------------+
   */

  /** Ensures single subscription happens in case of racing */
  @ParameterizedTest(name = "Should subscribe exactly once to {0}")
  @MethodSource("scenarios")
  public void shouldSubscribeExactlyOnce(Scenario scenario) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    for (int i = 0; i < 10000; i++) {
      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Supplier<Payload> payloadSupplier = () -> genericPayload(allocator);

      final Publisher<?> requestOperator =
          scenario.requestOperator(allocator, payloadSupplier, sender, activeStreams);

      StepVerifier stepVerifier =
          StepVerifier.create(sender)
              .assertNext(
                  frame -> {
                    FrameAssert frameAssert =
                        FrameAssert.assertThat(frame)
                            .isNotNull()
                            .hasNoFragmentsFollow()
                            .typeOf(scenario.requestType());
                    if (scenario.requestType() == METADATA_PUSH) {
                      frameAssert
                          .hasStreamIdZero()
                          .hasPayloadSize(METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
                          .hasMetadata(METADATA_CONTENT);
                    } else {
                      frameAssert
                          .hasClientSideStreamId()
                          .hasStreamId(1)
                          .hasPayloadSize(
                              METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                                  + DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
                          .hasMetadata(METADATA_CONTENT)
                          .hasData(DATA_CONTENT);
                    }
                    frameAssert.hasNoLeaks();

                    if (requestOperator instanceof FrameHandler) {
                      ((FrameHandler) requestOperator).handleComplete();
                    }
                  })
              .thenCancel()
              .verifyLater();

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () -> {
                        AssertSubscriber subscriber = new AssertSubscriber<>();
                        requestOperator.subscribe(subscriber);
                        subscriber.await().assertTerminated().assertNoError();
                      },
                      () -> {
                        AssertSubscriber subscriber = new AssertSubscriber<>();
                        requestOperator.subscribe(subscriber);
                        subscriber.await().assertTerminated().assertNoError();
                      }))
          .matches(
              t -> {
                Assertions.assertThat(t).hasMessageContaining("allows only a single Subscriber");
                return true;
              });

      stepVerifier.verify(Duration.ofSeconds(1));
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }

    allocator.assertHasNoLeaks();
  }

  /** Ensures single frame is sent only once racing between requests */
  @ParameterizedTest(name = "{0} should sent requestFrame exactly once if request(n) is racing")
  @MethodSource("scenarios")
  public void shouldSentRequestFrameOnceInCaseOfRequestRacing(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
        .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);

    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    for (int i = 0; i < 10000; i++) {
      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Supplier<Payload> payloadSupplier = () -> genericPayload(allocator);

      final Publisher<Payload> requestOperator =
          (Publisher<Payload>)
              scenario.requestOperator(allocator, payloadSupplier, sender, activeStreams);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(0);
      requestOperator.subscribe(assertSubscriber);

      RaceTestUtils.race(() -> assertSubscriber.request(1), () -> assertSubscriber.request(1));

      final ByteBuf sentFrame = sender.poll();

      if (scenario.requestType().hasInitialRequestN()) {
        if (RequestStreamFrameCodec.initialRequestN(sentFrame) == 1) {
          FrameAssert.assertThat(sender.poll())
              .isNotNull()
              .hasStreamId(1)
              .hasRequestN(1)
              .typeOf(REQUEST_N)
              .hasNoLeaks();
        } else {
          Assertions.assertThat(RequestStreamFrameCodec.initialRequestN(sentFrame)).isEqualTo(2);
        }
      }

      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata(METADATA_CONTENT)
          .hasData(DATA_CONTENT)
          .hasNoFragmentsFollow()
          .typeOf(scenario.requestType())
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      ((RequesterFrameHandler) requestOperator).handlePayload(response);
      ((RequesterFrameHandler) requestOperator).handleComplete();

      if (scenario.requestType() == REQUEST_CHANNEL) {
        ((CoreSubscriber) requestOperator).onComplete();
        FrameAssert.assertThat(sender.poll()).typeOf(COMPLETE).hasStreamId(1).hasNoLeaks();
      }

      assertSubscriber
          .assertTerminated()
          .assertValuesWith(
              p -> {
                Assertions.assertThat(p.release()).isTrue();
                Assertions.assertThat(p.refCnt()).isZero();
              });

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
    allocator.assertHasNoLeaks();
  }

  /**
   * Ensures that no ByteBuf is leaked if reassembly is starting and cancel is happening at the same
   * time
   */
  @ParameterizedTest(name = "Should have no leaks when {0} is canceled during reassembly")
  @MethodSource("scenarios")
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
        .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);

    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Supplier<Payload> payloadSupplier = () -> genericPayload(allocator);

      final Publisher<Payload> requestOperator =
          (Publisher<Payload>)
              scenario.requestOperator(allocator, payloadSupplier, sender, activeStreams);

      final AssertSubscriber<Payload> assertSubscriber = new AssertSubscriber<>(1);

      requestOperator.subscribe(assertSubscriber);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata(METADATA_CONTENT)
          .hasData(DATA_CONTENT)
          .hasNoFragmentsFollow()
          .typeOf(scenario.requestType())
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      int mtu = ThreadLocalRandom.current().nextInt(64, 256);
      Payload responsePayload = randomPayload(allocator);
      ArrayList<ByteBuf> fragments = prepareFragments(allocator, mtu, responsePayload);
      RaceTestUtils.race(
          assertSubscriber::cancel,
          () -> {
            FrameHandler frameHandler = (FrameHandler) requestOperator;
            int lastFragmentId = fragments.size() - 1;
            for (int j = 0; j < fragments.size(); j++) {
              ByteBuf frame = fragments.get(j);
              frameHandler.handleNext(frame, lastFragmentId != j, lastFragmentId == j);
              frame.release();
            }
          });

      List<Payload> values = assertSubscriber.values();
      if (!values.isEmpty()) {
        Assertions.assertThat(values)
                  .hasSize(1)
                  .first()
                  .matches(p -> {
                    Assertions.assertThat(p.sliceData())
                              .matches(bb -> ByteBufUtil.equals(bb, responsePayload.sliceData()));
                    Assertions.assertThat(p.hasMetadata())
                              .isEqualTo(responsePayload.hasMetadata());
                    Assertions.assertThat(p.sliceMetadata())
                              .matches(bb -> ByteBufUtil.equals(bb, responsePayload.sliceMetadata()));
                    Assertions.assertThat(p.release())
                              .isTrue();
                    Assertions.assertThat(p.refCnt())
                              .isZero();
                    return true;
                  });
      }

      if (!sender.isEmpty()) {
        if (scenario.requestType() != REQUEST_CHANNEL) {
          assertSubscriber.assertNotTerminated();
        }

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

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
      allocator.assertHasNoLeaks();
    }
  }

  /**
   * Ensures that in case we have element reassembling and then it happens the remote sends
   * (errorFrame) and downstream subscriber sends cancel() and we have racing between onError and
   * cancel we will not have any memory leaks
   */
  @ParameterizedTest(name = "Should have no leaks when {0} has cancel and error signals racing during reassembly")
  @MethodSource("scenarios")
  public void shouldHaveNoUnexpectedErrorDuringOnErrorAndCancelRacing(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
               .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);

    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final ArrayList<Throwable> droppedErrors = new ArrayList<>();
    Hooks.onErrorDropped(droppedErrors::add);
    try {
      for (int i = 0; i < 10000; i++) {
        final TestStreamManager activeStreams = TestStreamManager.client();
        final Supplier<Payload> payloadSupplier = () -> genericPayload(allocator);

        final Publisher<Payload> requestOperator =
                (Publisher<Payload>)
                        scenario.requestOperator(allocator, payloadSupplier, sender, activeStreams);

        final AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(1);
        requestOperator.subscribe(assertSubscriber);

        final ByteBuf sentFrame = sender.poll();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .hasPayloadSize(
                DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                    + METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
            .hasMetadata(METADATA_CONTENT)
            .hasData(DATA_CONTENT)
            .hasNoFragmentsFollow()
            .typeOf(scenario.requestType())
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();

          final ByteBuf fragmentBuf = allocator.buffer().writeBytes(new byte[] {1, 2, 3});
          ((FrameHandler) requestOperator).handleNext(fragmentBuf, true, false);
          // mimic frameHandler behaviour
          fragmentBuf.release();

        final RuntimeException testException = new RuntimeException("test");
        RaceTestUtils.race(
            assertSubscriber::cancel,
            () -> ((FrameHandler) requestOperator).handleError(testException));

        activeStreams.assertNoActiveStreams();
        if (!sender.isEmpty()) {
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

        droppedErrors.clear();
        allocator.assertHasNoLeaks();
      }
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
  @ParameterizedTest(name = "Should have no leaks when {0} has cancel racing with first request(1)")
  @MethodSource("scenarios")
  public void shouldBeConsistentInCaseOfRacingOfCancellationAndRequest(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
               .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Supplier<Payload> payloadSupplier = () -> genericPayload(allocator);

      final Publisher<Payload> requestOperator =
              (Publisher<Payload>)
                      scenario.requestOperator(allocator, payloadSupplier, sender, activeStreams);

      final AssertSubscriber<Payload> assertSubscriber = new AssertSubscriber<>(0);
      requestOperator.subscribe(assertSubscriber);

      RaceTestUtils.race(() -> assertSubscriber.cancel(), () -> assertSubscriber.request(1));

      if (!sender.isEmpty()) {
        final ByteBuf sentFrame = sender.poll();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .typeOf(scenario.requestType())
            .hasPayloadSize(
                DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                    + METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
            .hasMetadata(METADATA_CONTENT)
            .hasData(DATA_CONTENT)
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

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
      allocator.assertHasNoLeaks();
    }
  }

  /** Ensures that CancelFrame is sent exactly once in case of racing between cancel() methods */
  @ParameterizedTest(name = "Only as single frame(CANCEL) should be sent if racing cancel signals")
  @MethodSource("scenarios")
  public void shouldSentCancelFrameExactlyOnce(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
               .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);

    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 10000; i++) {
      final TestStreamManager activeStreams = TestStreamManager.client();
      final Supplier<Payload> payloadSupplier = () -> genericPayload(allocator);

      final Publisher<Payload> requestOperator =
              (Publisher<Payload>)
                      scenario.requestOperator(allocator, payloadSupplier, sender, activeStreams);

      final AssertSubscriber<Payload> assertSubscriber = new AssertSubscriber<>();
      requestOperator.subscribe(assertSubscriber);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .typeOf(scenario.requestType())
          .hasClientSideStreamId()
          .hasPayloadSize(
              DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + METADATA_CONTENT.getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata(METADATA_CONTENT)
          .hasData(DATA_CONTENT)
          .hasStreamId(1)
          .hasNoLeaks();

      RaceTestUtils.race(
          assertSubscriber::cancel, assertSubscriber::cancel);

      final ByteBuf cancelFrame = sender.poll();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      activeStreams.assertNoActiveStreams();
      assertSubscriber.assertNotTerminated();

      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
    allocator.assertHasNoLeaks();
  }

  static Payload genericPayload(LeaksTrackingByteBufAllocator allocator) {
    ByteBuf data = allocator.buffer();
    data.writeCharSequence(DATA_CONTENT, CharsetUtil.UTF_8);

    ByteBuf metadata = allocator.buffer();
    metadata.writeCharSequence(METADATA_CONTENT, CharsetUtil.UTF_8);

    return ByteBufPayload.create(data, metadata);
  }

  static Payload randomPayload(LeaksTrackingByteBufAllocator allocator) {
    boolean hasMetadata = ThreadLocalRandom.current().nextBoolean();
    ByteBuf metadataByteBuf;
    if (hasMetadata) {
      byte[] randomMetadata = new byte[ThreadLocalRandom.current().nextInt(0, 512)];
      ThreadLocalRandom.current().nextBytes(randomMetadata);
      metadataByteBuf = allocator.buffer().writeBytes(randomMetadata);
    } else {
      metadataByteBuf = null;
    }
    byte[] randomData = new byte[ThreadLocalRandom.current().nextInt(512, 1024)];
    ThreadLocalRandom.current().nextBytes(randomData);

    ByteBuf dataByteBuf = allocator.buffer().writeBytes(randomData);
    return ByteBufPayload.create(dataByteBuf, metadataByteBuf);
  }

  static ArrayList<ByteBuf> prepareFragments(
      LeaksTrackingByteBufAllocator allocator, int mtu, Payload payload) {
    boolean hasMetadata = payload.hasMetadata();
    ByteBuf data = payload.sliceData();
    ByteBuf metadata = payload.sliceMetadata();
    ArrayList<ByteBuf> fragments = new ArrayList<>();

    fragments.add(
        FragmentationUtils.encodeFirstFragment(
            allocator, mtu, FrameType.NEXT_COMPLETE, 1, hasMetadata, metadata, data));

    while (metadata.isReadable() || data.isReadable()) {
      fragments.add(
          FragmentationUtils.encodeFollowsFragment(allocator, mtu, 1, true, metadata, data));
    }

    return fragments;
  }
}
