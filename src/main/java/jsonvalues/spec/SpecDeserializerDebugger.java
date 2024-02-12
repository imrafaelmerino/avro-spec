package jsonvalues.spec;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import jdk.jfr.consumer.RecordedEvent;

/**
 * A consumer for Java Flight Recorder (JFR) events related to Avro serialization debugging. It prints out the details
 * of Avro serialization events, including the serializer name, result, number of errors, number of successes, duration,
 * exception details, thread information, and event start time.
 */
public final class SpecDeserializerDebugger implements Consumer<RecordedEvent> {

  /**
   * The singleton instance of {@code SpecSerializerDebugger}.
   */
  public static final SpecDeserializerDebugger INSTANCE = new SpecDeserializerDebugger();

  private SpecDeserializerDebugger() {
  }

  private static final String FORMAT = """
      event: avro-deserialization, deserializer: %s, result: %s, #errors: %s, #success: %s,
      duration: %s, exception: %s, thread: %s, event-start-time: %s
      """;

  /**
   * Accepts a recorded event and prints out the details of Avro serialization events.
   *
   * @param e The recorded event.
   */
  @Override
  public void accept(RecordedEvent e) {
    String exc = e.getValue("exceptionDetails");
    var str = String.format(FORMAT,
                            e.getValue("name"),
                            e.getValue("result"),
                            e.getValue("errorsCountersStat"),
                            e.getValue("successCounterStat"),
                            DebuggerUtils.formatTime(e.getDuration()
                                                      .toNanos()),
                            exc,
                            DebuggerUtils.getThreadName(e.getThread()),
                            e.getStartTime()
                             .atZone(ZoneId.systemDefault())
                             .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

                           );
    synchronized (System.out) {
      System.out.println(str);
      System.out.flush();
    }

  }
}
