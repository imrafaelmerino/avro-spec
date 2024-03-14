package jsonvalues.avro;

import jsonvalues.spec.JsSpec;
import jsonvalues.spec.serializers.confluent.avro.JsSpecSerializer;

public final class PaymentSerializer extends JsSpecSerializer {

  @Override
  protected boolean isJFREnabled() {
    return true;
  }

  @Override
  protected JsSpec getSpec() {
    return Specs.paymentSpec;
  }


}
