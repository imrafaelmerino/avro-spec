package jsonvalues.avro;

import jsonvalues.spec.JsSpec;
import jsonvalues.spec.deserializers.confluent.ConfluentObjSpecDeserializer;

public final class PaymentDeserializer extends ConfluentObjSpecDeserializer {

  @Override
  protected JsSpec getSpec() {
    return Specs.paymentSpec;
  }

  @Override
  protected boolean isJFREnabled() {
    return true;
  }
}
