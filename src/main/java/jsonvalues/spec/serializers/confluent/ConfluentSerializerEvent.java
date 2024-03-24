package jsonvalues.spec.serializers.confluent;

import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Confluent Spec Serializer Event")
@Name("Confluent_Avro_Serializer_Event")
@Category({"avro-spec", "Kafka", "Confluent", "Serializer"})
@Description("Event for tracking serialization of Avro Confluent Serializer")
final class ConfluentSerializerEvent extends Event {

  private static final AtomicLong serializerCounter = new AtomicLong(0);
  int bytes;
  String topic;
  String result;
  String exception;
  long counter = serializerCounter.incrementAndGet();

  enum RESULT {SUCCESS, FAILURE}

}
