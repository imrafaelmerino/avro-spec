package jsonvalues.spec.serializers;

import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Avro Serializer Event")
@Name("Avro_Serializer_Event")
@Category({"avro-spec", "Avro", "Serializer"})
@Description("Event for tracking serialization of Avro Serializer")
final class SerializerEvent extends Event {

  private static final AtomicLong serializerCounter = new AtomicLong(0);
  int bytes;
  long counter = serializerCounter.incrementAndGet();
  String result;
  String exception;

  enum RESULT {SUCCESS, FAILURE}

}
