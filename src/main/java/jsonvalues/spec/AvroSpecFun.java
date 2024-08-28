package jsonvalues.spec;

import java.util.Objects;
import java.util.function.Supplier;


/**
 * Utility class with static methods used in the implementation of the AvroSpec classes.
 */
public final class AvroSpecFun {

  private AvroSpecFun() {
  }

  /**
   * Finds the ultimate cause in the exception chain or the exception if the cause is null.
   *
   * @param exception The initial exception to start the search from.
   * @return The ultimate cause in the exception chain.
   * @throws NullPointerException If the provided exception is {@code null}.
   */
  public static Throwable findUltimateCause(Throwable exception) {
    var ultimateCause = Objects.requireNonNull(exception);

    while (ultimateCause.getCause() != null) {
      ultimateCause = ultimateCause.getCause();
    }

    return ultimateCause;
  }


  /**
   * Formats a given time duration in nanoseconds into a human-readable string.
   *
   * @param time The time in nanoseconds.
   * @return A formatted string representing the time duration.
   */
  public static String formatTime(long time) {
    if (time < 0) {
      throw new IllegalArgumentException("time < 0");
    }
    if (time >= 1000_000_000) {
      return "%.3f sg".formatted(time / 1000_000_000d);
    }
    if (time >= 1000_000) {
      return "%.3f ms".formatted(time / 1000_000d);
    }
    if (time >= 1000) {
      return "%.3f µs".formatted(time / 1000d);
    }
    return "%d ns".formatted(time);
  }


  static boolean debugNonNull(Object object) {
    if (object != null) {
      synchronized (System.out) {
        System.out.println(object);
      }
    }
    return object != null;
  }

  static boolean debugNonNull(Object object,
                              Supplier<String> message) {
    if (object != null) {
      synchronized (System.out) {
        System.out.println(message.get());
      }
    }
    return object != null;
  }
}
