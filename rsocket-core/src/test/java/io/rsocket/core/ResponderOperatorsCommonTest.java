package io.rsocket.core;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.PayloadAssert;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.publisher.TestPublisher;

import static io.rsocket.core.RequesterOperatorsRacingTest.genericPayload;
import static io.rsocket.core.RequesterOperatorsRacingTest.prepareFragments;
import static io.rsocket.core.RequesterOperatorsRacingTest.randomPayload;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameType.METADATA_PUSH;
import static io.rsocket.frame.FrameType.NEXT;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_FNF;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;

@SuppressWarnings("unchecked")
public class ResponderOperatorsCommonTest {

  interface Scenario {
    FrameType requestType();

    int maxElements();

    ResponderFrameHandler responseOperator(
            long initialRequestN,
            ByteBufAllocator allocator,
            Payload firstPayload,
            UnboundedProcessor<ByteBuf> sender,
            TestStreamManager streamManager,
            RSocket handler);


    ResponderFrameHandler responseOperator(
            long initialRequestN,
            ByteBufAllocator allocator,
            ByteBuf firstFragment,
            UnboundedProcessor<ByteBuf> sender,
            TestStreamManager streamManager,
            RSocket handler);
  }

  static Stream<Scenario> scenarios() {
    return Stream.of(
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_RESPONSE;
          }

            @Override
            public int maxElements() {
                return 1;
            }

            @Override
          public ResponderFrameHandler responseOperator(
                  long initialRequestN,
                  ByteBufAllocator allocator,
                  ByteBuf firstFragment,
                  UnboundedProcessor<ByteBuf> sender,
                  TestStreamManager streamManager,
                  RSocket handler) {
                int streamId = streamManager.getNextId();
                RequestResponseResponderSubscriber subscriber =
                        new RequestResponseResponderSubscriber(streamId,
                                allocator,
                                PayloadDecoder.ZERO_COPY,
                                firstFragment,
                                0,
                                FRAME_LENGTH_MASK,
                                Integer.MAX_VALUE,
                                streamManager,
                                sender,
                                handler);
                streamManager.activeStreams.put(streamId, subscriber);
                return subscriber;
          }

          @Override
          public ResponderFrameHandler responseOperator(
                  long initialRequestN,
                  ByteBufAllocator allocator,
                  Payload firstPayload,
                  UnboundedProcessor<ByteBuf> sender,
                  TestStreamManager streamManager,
                  RSocket handler) {
              int streamId = streamManager.getNextId();
              RequestResponseResponderSubscriber subscriber =
                      new RequestResponseResponderSubscriber(streamId,
                              allocator,
                              0,
                              FRAME_LENGTH_MASK,
                              Integer.MAX_VALUE,
                              streamManager,
                              sender);
              streamManager.activeStreams.put(streamId, subscriber);
              return handler
                .requestResponse(firstPayload)
                .subscribeWith(subscriber);
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
                public int maxElements() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public ResponderFrameHandler responseOperator(
                        long initialRequestN,
                        ByteBufAllocator allocator,
                        ByteBuf firstFragment,
                        UnboundedProcessor<ByteBuf> sender,
                        TestStreamManager streamManager,
                        RSocket handler) {
                    int streamId = streamManager.getNextId();
                    RequestStreamResponderSubscriber subscriber =
                            new RequestStreamResponderSubscriber(streamId,
                                    initialRequestN,
                                    allocator,
                                    PayloadDecoder.ZERO_COPY,
                                    firstFragment,
                                    0,
                                    FRAME_LENGTH_MASK,
                                    Integer.MAX_VALUE,
                                    streamManager,
                                    sender,
                                    handler);
                    streamManager.activeStreams.put(streamId, subscriber);
                    return subscriber;
                }

                @Override
                public ResponderFrameHandler responseOperator(
                        long initialRequestN,
                        ByteBufAllocator allocator,
                        Payload firstPayload,
                        UnboundedProcessor<ByteBuf> sender,
                        TestStreamManager streamManager,
                        RSocket handler) {
                    int streamId = streamManager.getNextId();
                    RequestStreamResponderSubscriber subscriber =
                            new RequestStreamResponderSubscriber(streamId,
                                    initialRequestN,
                                    allocator,
                                    0,
                                    FRAME_LENGTH_MASK,
                                    Integer.MAX_VALUE,
                                    streamManager,
                                    sender);
                    streamManager.activeStreams.put(streamId, subscriber);
                    return handler.requestStream(firstPayload)
                                  .subscribeWith(subscriber);
                }

                @Override
                public String toString() {
                    return RequestStreamResponderSubscriber.class.getSimpleName();
                }
            },
            new Scenario() {
                @Override
                public FrameType requestType() {
                    return FrameType.REQUEST_CHANNEL;
                }

                @Override
                public int maxElements() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public ResponderFrameHandler responseOperator(
                        long initialRequestN,
                        ByteBufAllocator allocator,
                        ByteBuf firstFragment,
                        UnboundedProcessor<ByteBuf> sender,
                        TestStreamManager streamManager,
                        RSocket handler) {
                    int streamId = streamManager.getNextId();
                    RequestChannelResponderSubscriber subscriber =
                            new RequestChannelResponderSubscriber(streamId,
                                    initialRequestN,
                                    allocator,
                                    PayloadDecoder.ZERO_COPY,
                                    firstFragment,
                                    0,
                                    FRAME_LENGTH_MASK,
                                    Integer.MAX_VALUE,
                                    streamManager,
                                    sender,
                                    handler);
                    streamManager.activeStreams.put(streamId, subscriber);
                    return subscriber;
                }

                @Override
                public ResponderFrameHandler responseOperator(
                        long initialRequestN,
                        ByteBufAllocator allocator,
                        Payload firstPayload,
                        UnboundedProcessor<ByteBuf> sender,
                        TestStreamManager streamManager,
                        RSocket handler) {
                    int streamId = streamManager.getNextId();
                    RequestChannelResponderSubscriber responderSubscriber =
                            new RequestChannelResponderSubscriber(streamId,
                                    initialRequestN,
                                    allocator,
                                    PayloadDecoder.ZERO_COPY,
                                    firstPayload,
                                    0,
                                    FRAME_LENGTH_MASK,
                                    Integer.MAX_VALUE,
                                    streamManager,
                                    sender);
                    streamManager.activeStreams.put(streamId, responderSubscriber);
                    return handler.requestChannel(responderSubscriber)
                            .subscribeWith(responderSubscriber);
                }

                @Override
                public String toString() {
                    return RequestChannelResponderSubscriber.class.getSimpleName();
                }
            });
  }

  static class TestHandler implements RSocket {

      final TestPublisher<Payload> producer;
      final AssertSubscriber<Payload> consumer;

      TestHandler(TestPublisher<Payload> producer, AssertSubscriber<Payload> consumer) {
          this.producer = producer;
          this.consumer = consumer;
      }

      @Override
      public Mono<Void> fireAndForget(Payload payload) {
          payload.release();
          return producer.mono().then();
      }

      @Override
      public Mono<Payload> requestResponse(Payload payload) {
          payload.release();
          return producer.mono();
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
          payload.release();
          return producer.flux();
      }

      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
          payloads.subscribe(consumer);
          return producer.flux();
      }
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void shouldHandleRequest(Scenario scenario) {
      Assumptions.assumeThat(scenario.requestType()).isNotIn(REQUEST_FNF, METADATA_PUSH);

      LeaksTrackingByteBufAllocator allocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
      TestPublisher<Payload> testPublisher = TestPublisher.create();
      TestHandler testHandler = new TestHandler(testPublisher, new AssertSubscriber<>(0));
      UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      TestStreamManager testStreamManager = TestStreamManager.client();

      ResponderFrameHandler responderFrameHandler =
              scenario.responseOperator(Long.MAX_VALUE, allocator,
                      genericPayload(allocator), sender,
                      testStreamManager, testHandler);

      Payload randomPayload = randomPayload(allocator);
      testPublisher.next(randomPayload.retain());
      testPublisher.complete();

      FrameAssert.assertThat(sender.poll())
                 .isNotNull()
                 .hasStreamId(1)
                 .typeOf(scenario.requestType() == REQUEST_RESPONSE ? FrameType.NEXT_COMPLETE : NEXT)
                 .hasPayloadSize(randomPayload.data().readableBytes() + randomPayload.sliceMetadata().readableBytes())
                 .hasData(randomPayload.data())
                 .hasNoLeaks();

      PayloadAssert.assertThat(randomPayload)
                   .hasNoLeaks();

      if (scenario.requestType() != REQUEST_RESPONSE) {

          FrameAssert.assertThat(sender.poll())
                     .typeOf(FrameType.COMPLETE)
                     .hasStreamId(1)
                     .hasNoLeaks();

          if (scenario.requestType() == REQUEST_CHANNEL) {
              testHandler.consumer.request(2);
              testHandler.consumer.assertValueCount(1)
                                  .assertValuesWith(p ->
                                          PayloadAssert.assertThat(p)
                                                       .hasNoLeaks()
                                  );
              FrameAssert.assertThat(sender.poll())
                         .typeOf(FrameType.REQUEST_N)
                         .hasStreamId(1)
                         .hasRequestN(1)
                         .hasNoLeaks();
          }
      }

      allocator.assertHasNoLeaks();
  }

    @ParameterizedTest
    @MethodSource("scenarios")
    void shouldHandleFragmentedRequest(Scenario scenario) {
        Assumptions.assumeThat(scenario.requestType()).isNotIn(REQUEST_FNF, METADATA_PUSH);

        LeaksTrackingByteBufAllocator allocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
        TestPublisher<Payload> testPublisher = TestPublisher.create();
        TestHandler testHandler = new TestHandler(testPublisher, new AssertSubscriber<>(0));
        UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
        TestStreamManager testStreamManager = TestStreamManager.client();

        Payload requestPayload = randomPayload(allocator);
        int mtu = ThreadLocalRandom.current()
                                 .nextInt(64, 256);
        Iterator<ByteBuf> fragments = prepareFragments(allocator, mtu, requestPayload).iterator();
        ByteBuf firstFragment = fragments.next();
        ResponderFrameHandler responderFrameHandler =
                scenario.responseOperator(Long.MAX_VALUE, allocator,
                        firstFragment, sender,
                        testStreamManager, testHandler);

        Payload randomPayload = randomPayload(allocator);
        testPublisher.next(randomPayload.retain());
        testPublisher.complete();

        FrameAssert.assertThat(sender.poll())
                   .isNotNull()
                   .hasStreamId(1)
                   .typeOf(scenario.requestType() == REQUEST_RESPONSE ? FrameType.NEXT_COMPLETE : NEXT)
                   .hasPayloadSize(randomPayload.data().readableBytes() + randomPayload.sliceMetadata().readableBytes())
                   .hasData(randomPayload.data())
                   .hasNoLeaks();

        PayloadAssert.assertThat(randomPayload)
                     .hasNoLeaks();

        if (scenario.requestType() != REQUEST_RESPONSE) {

            FrameAssert.assertThat(sender.poll())
                       .typeOf(FrameType.COMPLETE)
                       .hasStreamId(1)
                       .hasNoLeaks();

            if (scenario.requestType() == REQUEST_CHANNEL) {
                testHandler.consumer.request(2);
                testHandler.consumer.assertValueCount(1)
                                    .assertValuesWith(p ->
                                            PayloadAssert.assertThat(p)
                                                         .hasNoLeaks()
                                    );
                FrameAssert.assertThat(sender.poll())
                           .typeOf(FrameType.REQUEST_N)
                           .hasStreamId(1)
                           .hasRequestN(1)
                           .hasNoLeaks();
            }
        }

        allocator.assertHasNoLeaks();
    }
}
