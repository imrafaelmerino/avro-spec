package jsonvalues.avro.serializers;

import jsonvalues.avro.Specs;
import jsonvalues.spec.ConfluentAvroSerializer;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.SpecToAvroSchema;

public final class PeripheralAvroConfluentSerializer extends ConfluentAvroSerializer {



  @Override
  protected JsSpec getSpec() {
    return Specs.peripheralSpec;
  }

  @Override
  protected boolean isJFREnabled() {
    return true;
  }

  public static void main(String[] args) {
    System.out.println(SpecToAvroSchema.convert(Specs.peripheralSpec)
                                       .toString());
  }
}
