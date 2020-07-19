package io.rsocket.core;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class StateUtils {
  // todo add bits structure docs
  static final long FLAGS_MASK =
      0b111111111111111111111111111111111_0000000000000000000000000000000L;
  static final long REQUEST_MASK =
      0b000000000000000000000000000000000_1111111111111111111111111111111L;

  static final long SUBSCRIBED_FLAG =
      0b000000000000000000000000000000001_0000000000000000000000000000000L;
  static final long FIRST_FRAME_SENT_FLAG =
      0b000000000000000000000000000000010_0000000000000000000000000000000L;
  static final long REASSEMBLING_FLAG =
      0b000000000000000000000000000000100_0000000000000000000000000000000L;
  static final long INBOUND_TERMINATED_FLAG =
      0b000000000000000000000000000001000_0000000000000000000000000000000L;
  static final long OUTBOUND_TERMINATED_FLAG =
      0b000000000000000000000000000010000_0000000000000000000000000000000L;

  static final long UNSUBSCRIBED_STATE =
      0b000000000000000000000000000000000_0000000000000000000000000000000L;
  static final long TERMINATED_STATE =
      0b100000000000000000000000000000000_0000000000000000000000000000000L;

  /**
   * Adds (if possible) to the given state the {@link #SUBSCRIBED_FLAG} flag which indicates that
   * the given stream has already been subscribed once
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been subscribed once
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markSubscribed(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & SUBSCRIBED_FLAG) == SUBSCRIBED_FLAG) {
        return state;
      }

      if (updater.compareAndSet(instance, state, state | SUBSCRIBED_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that the given stream has already been subscribed once
   *
   * @param state to check whether stream is subscribed
   * @return true if the {@link #SUBSCRIBED_FLAG} flag is set
   */
  static boolean isSubscribed(long state) {
    return (state & SUBSCRIBED_FLAG) == SUBSCRIBED_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #FIRST_FRAME_SENT_FLAG} flag which indicates
   * that the first frame has already set and logical stream has already been established.
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been established once
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markFirstFrameSent(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & FIRST_FRAME_SENT_FLAG) == FIRST_FRAME_SENT_FLAG) {
        return state;
      }

      if (updater.compareAndSet(instance, state, state | FIRST_FRAME_SENT_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that the first frame which established logical stream has already been sent
   *
   * @param state to check whether stream is established
   * @return true if the {@link #FIRST_FRAME_SENT_FLAG} flag is set
   */
  static boolean isFirstFrameSent(long state) {
    return (state & FIRST_FRAME_SENT_FLAG) == FIRST_FRAME_SENT_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #REASSEMBLING_FLAG} flag which indicates that
   * there is a payload reassembling in progress.
   *
   * <p>Note, the flag will not be added if the stream has already been terminated
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markReassembling(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if (updater.compareAndSet(instance, state, state | REASSEMBLING_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Removes (if possible) from the given state the {@link #REASSEMBLING_FLAG} flag which indicates
   * that a payload reassembly process is completed.
   *
   * <p>Note, the flag will not be removed if the stream has already been terminated
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markReassembled(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if (updater.compareAndSet(instance, state, state & ~REASSEMBLING_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that a payload reassembly process is completed.
   *
   * @param state to check whether there is reassembly in progress
   * @return true if the {@link #REASSEMBLING_FLAG} flag is set
   */
  static boolean isReassembling(long state) {
    return (state & REASSEMBLING_FLAG) == REASSEMBLING_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #INBOUND_TERMINATED_FLAG} flag which indicates
   * that an inbound channel of a bidirectional stream is terminated.
   *
   * <p><b>Note</b>, this action will have no effect if the stream has already been terminated or if
   * the {@link #INBOUND_TERMINATED_FLAG} flag has already been set. <br>
   * <b>Note</b>, if the outbound stream has already been terminated, then the result state will be
   * {@link #TERMINATED_STATE}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markInboundTerminated(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & INBOUND_TERMINATED_FLAG) == INBOUND_TERMINATED_FLAG) {
        return state;
      }

      if ((state & OUTBOUND_TERMINATED_FLAG) == OUTBOUND_TERMINATED_FLAG) {
        if (updater.compareAndSet(instance, state, TERMINATED_STATE)) {
          return state;
        }
      } else {
        if (updater.compareAndSet(instance, state, state | INBOUND_TERMINATED_FLAG)) {
          return state;
        }
      }
    }
  }

  /**
   * Indicates that a the inbound channel of a bidirectional stream is terminated.
   *
   * @param state to check whether it has {@link #INBOUND_TERMINATED_FLAG} set
   * @return true if the {@link #INBOUND_TERMINATED_FLAG} flag is set
   */
  static boolean isInboundTerminated(long state) {
    return (state & INBOUND_TERMINATED_FLAG) == INBOUND_TERMINATED_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #OUTBOUND_TERMINATED_FLAG} flag which
   * indicates that an outbound channel of a bidirectional stream is terminated.
   *
   * <p><b>Note</b>, this action will have no effect if the stream has already been terminated or if
   * the {@link #OUTBOUND_TERMINATED_FLAG} flag has already been set. <br>
   * <b>Note</b>, if the {@code checkEstablishment} parameter is {@code true} and the logical stream
   * is not established, then the result state will be {@link #TERMINATED_STATE} <br>
   * <b>Note</b>, if the inbound stream has already been terminated, then the result state will be
   * {@link #TERMINATED_STATE}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param checkEstablishment indicates whether {@link #FIRST_FRAME_SENT_FLAG} should be checked to
   *     make final decision
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markOutboundTerminated(
      AtomicLongFieldUpdater<T> updater, T instance, boolean checkEstablishment) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & OUTBOUND_TERMINATED_FLAG) == OUTBOUND_TERMINATED_FLAG) {
        return state;
      }

      if ((checkEstablishment && !isFirstFrameSent(state))
          || (state & INBOUND_TERMINATED_FLAG) == INBOUND_TERMINATED_FLAG) {
        if (updater.compareAndSet(instance, state, TERMINATED_STATE)) {
          return state;
        }
      } else {
        if (updater.compareAndSet(instance, state, state | OUTBOUND_TERMINATED_FLAG)) {
          return state;
        }
      }
    }
  }

  /**
   * Indicates that a the outbound channel of a bidirectional stream is terminated.
   *
   * @param state to check whether it has {@link #OUTBOUND_TERMINATED_FLAG} set
   * @return true if the {@link #OUTBOUND_TERMINATED_FLAG} flag is set
   */
  static boolean isOutboundTerminated(long state) {
    return (state & OUTBOUND_TERMINATED_FLAG) == OUTBOUND_TERMINATED_FLAG;
  }

  /**
   * Makes current state a {@link #TERMINATED_STATE}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markTerminated(AtomicLongFieldUpdater<T> updater, T instance) {
    return updater.getAndSet(instance, TERMINATED_STATE);
  }

  /**
   * Makes current state a {@link #TERMINATED_STATE} using {@link
   * AtomicLongFieldUpdater#lazySet(Object, long)}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   */
  static <T> void lazyTerminate(AtomicLongFieldUpdater<T> updater, T instance) {
    updater.lazySet(instance, TERMINATED_STATE);
  }

  /**
   * Indicates that a the outbound channel of a bidirectional stream is terminated.
   *
   * @param state to check whether it has {@link #OUTBOUND_TERMINATED_FLAG} set
   * @return true if the {@link #OUTBOUND_TERMINATED_FLAG} flag is set
   */
  static boolean isTerminated(long state) {
    return state == TERMINATED_STATE;
  }

  /**
   * @param updater
   * @param instance
   * @param toAdd
   * @param <T>
   * @return
   */
  static <T> long addRequestN(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
    long currentState, flags, requestN, nextRequestN;
    for (; ; ) {
      currentState = updater.get(instance);

      if (currentState == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      requestN = currentState & REQUEST_MASK;
      if (requestN == REQUEST_MASK) {
        return currentState;
      }

      flags = currentState & FLAGS_MASK;
      nextRequestN = addRequestN(requestN, toAdd);

      if (updater.compareAndSet(instance, currentState, nextRequestN | flags)) {
        return currentState;
      }
    }
  }

  static long addRequestN(long a, long b) {
    long res = a + b;
    if (res < 0 || res > REQUEST_MASK) {
      return REQUEST_MASK;
    }
    return res;
  }

  static boolean isWorkInProgress(long state) {
    return (state & REQUEST_MASK) > 0;
  }

  static long extractRequestN(long state) {
    long requestN = state & REQUEST_MASK;

    if (requestN == REQUEST_MASK) {
      return REQUEST_MASK;
    }

    return requestN;
  }

  static boolean isMaxAllowedRequestN(long n) {
    return n >= REQUEST_MASK;
  }
}
