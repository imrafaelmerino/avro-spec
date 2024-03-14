package jsonvalues.avro;

import jsonvalues.spec.JsSpec;
import jsonvalues.spec.deserializers.confluent.avro.JsObjSpecDeserializer;

public final class PaymentDeserializer extends JsObjSpecDeserializer {

  @Override
  protected JsSpec getSpec() {
    return Specs.paymentSpec;
  }

  @Override
  protected boolean isJFREnabled() {
    return true;
  }
}
