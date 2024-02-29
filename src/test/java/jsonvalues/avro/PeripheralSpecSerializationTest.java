package jsonvalues.avro;

import jsonvalues.JsObj;
import jsonvalues.JsValue;
import jsonvalues.spec.ObjSpecDeserializer;
import jsonvalues.spec.ObjSpecDeserializerBuilder;
import jsonvalues.spec.SpecSerializer;
import jsonvalues.spec.SpecSerializerBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class PeripheralSpecSerializationTest {


  @Test
  public void testSerialization() {

    SpecSerializer serializer =
        SpecSerializerBuilder.of(Specs.peripheralSpec)
                             .build();
    ObjSpecDeserializer deserializer =
        ObjSpecDeserializerBuilder.of(Specs.peripheralSpec,
                                      Specs.peripheralSpec)
                                  .build();

    Gens.peripheralGen.sample(10)
                      .peek(System.out::println)
                      .forEach(obj -> {
                                 byte[] serialized = serializer.binaryEncode(obj);

                                 JsObj a = deserializer.binaryDecode(serialized);

                                 Assertions.assertEquals(obj,
                                                         a.filterValues(JsValue::isNotNull));


                               }
                              );


  }


}