package jsonvalues.avro;

import java.time.Duration;
import jio.test.junit.Debugger;
import jsonvalues.spec.SpecDeserializerDebugger;
import jsonvalues.spec.SpecSerializerDebugger;
import org.junit.jupiter.api.extension.RegisterExtension;

public final class MyDebuggers {

  private MyDebuggers() {
  }

  public static Debugger avroDebugger(Duration d) {

    Debugger debugger = Debugger.of(d);
    debugger.registerEventConsumer("AvroSpecDeserializerEvent",
                                   SpecDeserializerDebugger.INSTANCE);
    debugger.registerEventConsumer("AvroSpecSerializerEvent",
                                   SpecSerializerDebugger.INSTANCE);

    return debugger;
  }

  @RegisterExtension
  static Debugger debugger = avroDebugger(Duration.ofSeconds(2));


}
