package jsonvalues.avro;

import jsonvalues.spec.JsSpec;
import jsonvalues.spec.serializers.confluent.ConfluentSpecSerializer;

public final class PaymentSerializer extends ConfluentSpecSerializer {

  @Override
  protected boolean isJFREnabled() {
    return true;
  }

  @Override
  protected JsSpec getSpec() {
    return Specs.paymentSpec;
  }


}
