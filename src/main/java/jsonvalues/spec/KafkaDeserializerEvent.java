package jsonvalues.spec;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Avro Spec Deserializer Event")
@Name("Avro_Deserializer_Event")
@Category({"avro-spec", "Kafka", "Deserializer"})
@Description("Event for tracking deserialization of avro-spec Deserializers")
class KafkaDeserializerEvent extends Event {

  String schema;
  String topic;
  long counter;
  String result;
  String exception;


  enum RESULT {SUCCESS, FAILURE}

}
