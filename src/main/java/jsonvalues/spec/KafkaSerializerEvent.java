package jsonvalues.spec;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Avro Spec Serializer Event")
@Name("Avro_Serializer_Event")
@Category({"avro-spec", "Kafka", "Serializer"})
@Description("Event for tracking serialization of avro-spec Serializers")
class KafkaSerializerEvent extends Event {

  String schema;
  String topic;
  long counter;
  String result;
  String exception;


  private enum RESULT {SUCCESS, FAILURE}

}
