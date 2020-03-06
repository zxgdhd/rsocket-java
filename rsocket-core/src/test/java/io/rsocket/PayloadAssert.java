package io.rsocket;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.frame.ByteBufRepresentation;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.MetadataPushFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.util.DefaultPayload;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.Objects;
import reactor.util.annotation.Nullable;

import static org.assertj.core.error.ShouldBe.shouldBe;
import static org.assertj.core.error.ShouldBeEqual.shouldBeEqual;
import static org.assertj.core.error.ShouldHave.shouldHave;
import static org.assertj.core.error.ShouldNotHave.shouldNotHave;

public class PayloadAssert extends AbstractAssert<PayloadAssert, Payload> {

  public static PayloadAssert assertThat(@Nullable Payload payload) {
    return new PayloadAssert(payload);
  }

  private final Failures failures = Failures.instance();

  public PayloadAssert(@Nullable Payload payload) {
    super(payload, PayloadAssert.class);
  }

  public PayloadAssert hasMetadata() {
    assertValid();

    if (!actual.hasMetadata()) {
      throw failures.failure(info, shouldHave(actual, new Condition<>("metadata present")));
    }

    return this;
  }

  public PayloadAssert hasNoMetadata() {
    assertValid();

    if (actual.hasMetadata()) {
      throw failures.failure(info, shouldHave(actual, new Condition<>("metadata absent")));
    }

    return this;
  }

  public PayloadAssert hasMetadata(String metadata, Charset charset) {
    return hasMetadata(metadata.getBytes(charset));
  }

  public PayloadAssert hasMetadata(String metadataUtf8) {
    return hasMetadata(metadataUtf8, CharsetUtil.UTF_8);
  }

  public PayloadAssert hasMetadata(byte[] metadata) {
    return hasMetadata(Unpooled.wrappedBuffer(metadata));
  }

  public PayloadAssert hasMetadata(ByteBuf metadata) {
    hasMetadata();

    ByteBuf content = actual.sliceMetadata();
    if (!ByteBufUtil.equals(content, metadata)) {
      throw failures.failure(info, shouldBeEqual(content, metadata, new ByteBufRepresentation()));
    }

    return this;
  }

  public PayloadAssert hasData(String dataUtf8) {
    return hasData(dataUtf8, CharsetUtil.UTF_8);
  }

  public PayloadAssert hasData(String data, Charset charset) {
    return hasData(data.getBytes(charset));
  }

  public PayloadAssert hasData(byte[] data) {
    return hasData(Unpooled.wrappedBuffer(data));
  }

  public PayloadAssert hasData(ByteBuf data) {
    assertValid();

    ByteBuf content = actual.sliceData();
    if (!ByteBufUtil.equals(content, data)) {
      throw failures.failure(info, shouldBeEqual(content, data, new ByteBufRepresentation()));
    }

    return this;
  }

  public void hasNoLeaks() {
    if (!(actual instanceof DefaultPayload) && (!actual.release() || actual.refCnt() > 0)) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have refCnt(0) after release but "
                  + "actual was "
                  + "%n<refCnt(%s)>",
              actual, actual.refCnt()));
    }
  }

  private void assertValid() {
    Objects.instance().assertNotNull(info, actual);
  }
}
