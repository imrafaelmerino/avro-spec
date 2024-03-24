package jsonvalues.avro;

import jsonvalues.spec.JsSpec;
import jsonvalues.spec.deserializers.confluent.ConfluentArraySpecDeserializer;

public final class ArrayPaymentDeserializer extends ConfluentArraySpecDeserializer {

  @Override
  protected JsSpec getSpec() {
    return Specs.arrayPaymentSpec;
  }

  @Override
  protected boolean isJFREnabled() {
    return true;
  }
}
