package io.rsocket.core;

import static io.rsocket.core.ShouldHaveFlag.shouldHaveFlag;
import static io.rsocket.core.ShouldHaveFlag.shouldHaveRequestN;
import static io.rsocket.core.ShouldNotHaveFlag.shouldNotHaveFlag;
import static io.rsocket.core.StateUtils.FIRST_FRAME_SENT_FLAG;
import static io.rsocket.core.StateUtils.REASSEMBLING_FLAG;
import static io.rsocket.core.StateUtils.SUBSCRIBED_FLAG;
import static io.rsocket.core.StateUtils.TERMINATED_STATE;
import static io.rsocket.core.StateUtils.UNSUBSCRIBED_STATE;
import static io.rsocket.core.StateUtils.extractRequestN;
import static io.rsocket.core.StateUtils.isFirstFrameSent;
import static io.rsocket.core.StateUtils.isReassembling;
import static io.rsocket.core.StateUtils.isSubscribed;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.internal.Failures;

public class StateMachineAssert<T>
    extends AbstractAssert<StateMachineAssert<T>, AtomicLongFieldUpdater<T>> {

  public static <T> StateMachineAssert<T> assertThat(
      AtomicLongFieldUpdater<T> updater, T instance) {
    return new StateMachineAssert<>(updater, instance);
  }

  public static StateMachineAssert<FireAndForgetRequesterMono> assertThat(
      FireAndForgetRequesterMono instance) {
    return new StateMachineAssert<>(FireAndForgetRequesterMono.STATE, instance);
  }

  public static StateMachineAssert<RequestResponseRequesterMono> assertThat(
      RequestResponseRequesterMono instance) {
    return new StateMachineAssert<>(RequestResponseRequesterMono.STATE, instance);
  }

  public static StateMachineAssert<RequestStreamRequesterFlux> assertThat(
      RequestStreamRequesterFlux instance) {
    return new StateMachineAssert<>(RequestStreamRequesterFlux.STATE, instance);
  }

  private final Failures failures = Failures.instance();
  private final T instance;

  public StateMachineAssert(AtomicLongFieldUpdater<T> updater, T instance) {
    super(updater, StateMachineAssert.class);
    this.instance = instance;
  }

  public StateMachineAssert<T> isUnsubscribed() {
    long currentState = actual.get(instance);
    if (isSubscribed(currentState) || StateUtils.isTerminated(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, UNSUBSCRIBED_STATE));
    }
    return this;
  }

  public StateMachineAssert<T> hasSubscribedFlagOnly() {
    long currentState = actual.get(instance);
    if (currentState != SUBSCRIBED_FLAG) {
      throw failures.failure(info, shouldHaveFlag(currentState, SUBSCRIBED_FLAG));
    }
    return this;
  }

  public StateMachineAssert<T> hasSubscribedFlag() {
    long currentState = actual.get(instance);
    if (!isSubscribed(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, SUBSCRIBED_FLAG));
    }
    return this;
  }

  public StateMachineAssert<T> hasRequestN(long n) {
    long currentState = actual.get(instance);
    if (extractRequestN(currentState) != n) {
      throw failures.failure(info, shouldHaveRequestN(currentState, n));
    }
    return this;
  }

  public StateMachineAssert<T> hasFirstFrameSentFlag() {
    long currentState = actual.get(instance);
    if (!isFirstFrameSent(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, FIRST_FRAME_SENT_FLAG));
    }
    return this;
  }

  public StateMachineAssert<T> hasReassemblingFlag() {
    long currentState = actual.get(instance);
    if (!isReassembling(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, REASSEMBLING_FLAG));
    }
    return this;
  }

  public StateMachineAssert<T> hasNoReassemblingFlag() {
    long currentState = actual.get(instance);
    if (isReassembling(currentState)) {
      throw failures.failure(info, shouldNotHaveFlag(currentState, REASSEMBLING_FLAG));
    }
    return this;
  }

  public StateMachineAssert<T> isTerminated() {
    long currentState = actual.get(instance);
    if (!StateUtils.isTerminated(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, TERMINATED_STATE));
    }
    return this;
  }
}
