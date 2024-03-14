package jsonvalues.spec.deserializers.confluent.avro;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedThread;
import jsonvalues.spec.AvroSpecFun;

/**
 * A consumer for Java Flight Recorder (JFR) events related to Avro serialization debugging. It prints out the details
 * of Avro serialization events, including the serializer name, result, number of errors, number of successes, duration,
 * exception details, thread information, and event start time.
 */
public final class DeserializerDebugger implements Consumer<RecordedEvent> {

  /**
   * The singleton instance of {@code SpecSerializerDebugger}.
   */
  public static final DeserializerDebugger INSTANCE = new DeserializerDebugger();

  private DeserializerDebugger() {
  }

  @SuppressWarnings("InlineFormatString")
  private static final String FORMAT_SUC = """
      ------ Confluent-Avro-Deserializer -----
      |  Result: %s
      |  Topic: %s
      |  Duration: %s
      |  Bytes: %s
      |  Counter: %s
      |  Thread: %s
      |  Event Start Time: %s
      ----------------------
      """;
  @SuppressWarnings("InlineFormatString")
  private static final String FORMAT_ERR = """
      ------ Confluent-Avro-Deserializer -----
      |  Result: %s
      |  Topic: %s
      |  Exception: %s
      |  Duration: %s
      |  Bytes: %s
      |  Counter: %s
      |  Thread: %s
      |  Event Start Time: %s
      ----------------------
      """;
  static final String EVENT_NAME = "Confluent_Avro_Deserializer_Event";

  @Override
  public void accept(RecordedEvent event) {
    assert EVENT_NAME.equals(event.getEventType()
                                  .getName());
    var result = event.getValue("result");
    var topic = event.getValue("topic");
    boolean isSuccess = "SUCCESS".equals(result);
    RecordedThread thread = event.getThread();
    var str = isSuccess ?
              String.format(FORMAT_SUC,
                            result,
                            topic,
                            AvroSpecFun.formatTime(event.getDuration()
                                                        .toNanos()),

                            event.getValue("bytes"),
                            event.getValue("counter"),
                            thread != null ? thread.getJavaName() : "null",
                            event.getStartTime()
                                 .atZone(ZoneOffset.UTC)
                                 .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                           ) :
              String.format(FORMAT_ERR,
                            result,
                            topic,
                            event.getValue("exception"),
                            AvroSpecFun.formatTime(event.getDuration()
                                                        .toNanos()),
                            event.getValue("bytes"),
                            event.getValue("counter"),
                            thread != null ? thread.getJavaName() : "null",
                            event.getStartTime()
                                 .atZone(ZoneOffset.UTC)
                                 .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                           );
    synchronized (System.out) {
      System.out.println(str);
      System.out.flush();
    }
  }
}
