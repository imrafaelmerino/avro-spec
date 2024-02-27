package jsonvalues.spec;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Confluent Spec Serializer Event")
@Name("Confluent_Serializer_Event")
@Category({"avro-spec", "Kafka", "Confluent", "Serializer"})
@Description("Event for tracking serialization of avro-spec confluent Serializers")
class ConfluentSerializerEvent extends Event {

  String topic;

  String schema;

  public String result;

  String exception;
  long counter;


   enum RESULT {SUCCESS, FAILURE}

}
