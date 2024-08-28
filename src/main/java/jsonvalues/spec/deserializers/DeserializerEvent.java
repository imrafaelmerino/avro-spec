package jsonvalues.spec.deserializers;

import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Avro Deserializer Event")
@Name("Avro_Deserializer_Event")
@Category({"avro-spec", "Avro", "Deserializer"})
@Description("Event for tracking deserialization of Avro Deserializer")
final class DeserializerEvent extends Event {

  private static final AtomicLong deserializerCounter = new AtomicLong(0);
  int bytes;
  long counter = deserializerCounter.incrementAndGet();
  String result;
  String exception;

  enum RESULT {SUCCESS, FAILURE}

}
