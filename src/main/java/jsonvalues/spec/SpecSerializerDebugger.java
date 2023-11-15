package jsonvalues.spec;

import jdk.jfr.consumer.RecordedEvent;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * A {@code Consumer<RecordedEvent>} implementation for handling Avro serialization debugging events. It prints
 * information about Avro serialization events, such as serializer name, result, error count, success count, duration,
 * exception details, thread information, and event start time.
 *
 * <p>This class is designed to be used as a singleton, and the single instance is provided through the
 * {@code INSTANCE} constant.
 */
public final class SpecSerializerDebugger implements Consumer<RecordedEvent> {

    /**
     * Singleton instance of {@code SpecSerializerDebugger}.
     */
    public static final SpecSerializerDebugger INSTANCE = new SpecSerializerDebugger();
    private static final String FORMAT = """
            event: avro-serialization, serializer: %s, result: %s, #errors: %s, #success: %s,
            duration: %s, exception: %s, thread: %s, event-start-time: %s
            """;

    private SpecSerializerDebugger() {
    }

    /**
     * Handles the recorded event by printing formatted information about Avro serialization.
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
                                DebuggerUtils.formatTime(e.getDuration().toNanos()),
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
