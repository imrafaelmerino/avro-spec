package jsonvalues.spec;

import jdk.jfr.consumer.RecordedThread;


final class DebuggerUtils {

  private DebuggerUtils() {
  }

  /**
   * Formats a given time duration in nanoseconds into a human-readable string.
   *
   * @param time The time in nanoseconds.
   * @return A formatted string representing the time duration.
   */
  static String formatTime(long time) {
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


  /**
   * Retrieves the name of a recorded thread. If the recorded thread is null, the method returns "not recorded". If the
   * Java name of the thread is empty, it returns a formatted string using the operating system (OS) thread ID with a
   * prefix "virtual-".
   *
   * @param thread The recorded thread for which to retrieve the name.
   * @return The name of the recorded thread or a default string if the thread is not recorded.
   */
  static String getThreadName(final RecordedThread thread) {
    //happens at times that is null (don't know when)
     if (thread == null) {
        return "not recorded";
     }
    String javaName = thread.getJavaName();
    return javaName.isEmpty() ? "virtual-%s".formatted(thread.getOSThreadId()) : javaName;
  }
}
