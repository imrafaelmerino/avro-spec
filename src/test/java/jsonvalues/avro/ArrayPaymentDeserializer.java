package jsonvalues.avro;

import jsonvalues.spec.JsSpec;
import jsonvalues.spec.deserializers.confluent.avro.JsArraySpecDeserializer;
import jsonvalues.spec.deserializers.confluent.avro.JsObjSpecDeserializer;

public final class ArrayPaymentDeserializer extends JsArraySpecDeserializer {

  @Override
  protected JsSpec getSpec() {
    return Specs.arrayPaymentSpec;
  }

  @Override
  protected boolean isJFREnabled() {
    return true;
  }
}
