package jsonvalues.avro;

import java.time.Duration;
import jio.test.junit.Debugger;
import jsonvalues.spec.deserializers.avro.AvroDeserializerDebugger;
import jsonvalues.spec.deserializers.confluent.avro.ConfluentAvroDeserializerDebugger;
import jsonvalues.spec.serializers.avro.AvroSerializerDebugger;
import jsonvalues.spec.serializers.confluent.avro.ConfluentAvroSerializerDebugger;
import org.junit.jupiter.api.extension.RegisterExtension;

public final class MyDebuggers {

  private MyDebuggers() {
  }

  public static Debugger avroDebugger(Duration d) {

    Debugger debugger = Debugger.of(d);
    debugger.registerEventConsumer("Avro_Serializer_Event",
                                   AvroSerializerDebugger.INSTANCE);
    debugger.registerEventConsumer("Confluent_Avro_Serializer_Event",
                                   ConfluentAvroSerializerDebugger.INSTANCE);
    debugger.registerEventConsumer("Confluent_Avro_Deserializer_Event",
                                   ConfluentAvroDeserializerDebugger.INSTANCE);
    debugger.registerEventConsumer("Avro_Deserializer_Event",
                                   AvroDeserializerDebugger.INSTANCE);

    return debugger;
  }




}
