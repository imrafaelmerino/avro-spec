package jsonvalues.spec;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Confluent Spec Deserializer Event")
@Name("Confluent_Deserializer_Event")
@Category({"avro-spec", "Kafka", "Confluent", "Deserializer"})
@Description("Event for tracking deserialization of avro-spec confluent Deserializers")
class ConfluentDeserializerEvent extends Event {

  String schema;
  String topic;
  long counter;
  String result;
  String exception;

  enum RESULT {SUCCESS, FAILURE}

}
