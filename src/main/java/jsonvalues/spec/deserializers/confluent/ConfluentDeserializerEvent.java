package jsonvalues.spec.deserializers.confluent;

import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Confluent Avro Deserializer Event")
@Name("Confluent_Avro_Deserializer_Event")
@Category({"avro-spec", "Kafka", "Confluent", "Deserializer"})
@Description("Event for tracking deserialization of Avro Confluent Deserializer")
final class ConfluentDeserializerEvent extends Event {

  private static final AtomicLong deserializerCounter = new AtomicLong(0);
  int bytes;
  String topic;
  long counter = deserializerCounter.incrementAndGet();
  String result;
  String exception;
  enum RESULT {SUCCESS, FAILURE}

}
