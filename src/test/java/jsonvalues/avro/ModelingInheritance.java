package jsonvalues.avro;

import fun.gen.Combinators;
import fun.gen.Gen;
import fun.gen.NamedGen;
import jsonvalues.JsNull;
import jsonvalues.JsObj;
import jsonvalues.JsStr;
import jsonvalues.gen.*;
import jsonvalues.spec.*;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static jsonvalues.spec.JsSpecs.oneSpecOf;


public class ModelingInheritance {
    String NAME_FIELD = "name";
    String TYPE_FIELD = "type";
    String BUTTON_COUNT_FIELD = "buttonCount";
    String WHEEL_COUNT_FIELD = "wheelCount";
    String TRACKING_TYPE_FIELD = "trackingType";
    String KEY_COUNT_FIELD = "keyCount";
    String MEDIA_BUTTONS_FIELD = "mediaButtons";
    String CONNECTED_DEVICES_FIELD = "connectedDevices";
    String PERIPHERAL_FIELD = "peripheral";
    List<String> TRACKING_TYPE_ENUM = List.of("ball", "optical");

    @Test
    public void test() {

        var baseSpec = JsObjSpec.of(NAME_FIELD, JsSpecs.str(),
                                    TYPE_FIELD, JsEnumBuilder.withName("type")
                                                             .build("mouse", "keyboard", "usb_hub"));

        var baseGen = JsObjGen.of(NAME_FIELD, JsStrGen.alphabetic());

        var mouseSpec =
                JsObjSpecBuilder.withName("mouse")
                                .build(JsObjSpec.of(BUTTON_COUNT_FIELD, JsSpecs.integer(),
                                                    WHEEL_COUNT_FIELD, JsSpecs.integer(),
                                                    TRACKING_TYPE_FIELD, JsEnumBuilder.withName("tracking_type")
                                                                                      .build(TRACKING_TYPE_ENUM)
                                                   ))
                                .concat(baseSpec);

        var mouseGen =
                JsObjGen.of(BUTTON_COUNT_FIELD, JsIntGen.arbitrary(0, 10),
                            WHEEL_COUNT_FIELD, JsIntGen.arbitrary(0, 10),
                            TRACKING_TYPE_FIELD, Combinators.oneOf(TRACKING_TYPE_ENUM).map(JsStr::of),
                            TYPE_FIELD, Gen.cons(JsStr.of("mouse"))
                           )
                        .concat(baseGen);

        var keyboardSpec =
                JsObjSpecBuilder.withName("keyboard")
                                .build(JsObjSpec.of(KEY_COUNT_FIELD, JsSpecs.integer(),
                                                    MEDIA_BUTTONS_FIELD, JsSpecs.bool()
                                                   ))
                                .concat(baseSpec);

        var keyboardGen =

                JsObjGen.of(KEY_COUNT_FIELD, JsIntGen.arbitrary(0, 10),
                            MEDIA_BUTTONS_FIELD, JsBoolGen.arbitrary(),
                            TYPE_FIELD, Gen.cons(JsStr.of("keyboard"))
                           )
                        .concat(baseGen);


        var usbHubSpec =
                JsObjSpecBuilder.withName("usb_hub")
                                .withFieldsDefaults(Map.of(CONNECTED_DEVICES_FIELD, JsNull.NULL))
                                .build(JsObjSpec.of(CONNECTED_DEVICES_FIELD,
                                                    JsSpecs.arrayOfSpec(JsSpecs.ofNamedSpec(PERIPHERAL_FIELD)).nullable()
                                                   )
                                                .withOptKeys(CONNECTED_DEVICES_FIELD)
                                                .concat(baseSpec));

        var usbHubGen =
                JsObjGen.of(CONNECTED_DEVICES_FIELD,
                            JsArrayGen.biased(NamedGen.of(PERIPHERAL_FIELD), 2, 10),
                            TYPE_FIELD, Gen.cons(JsStr.of("usb_hub"))
                           )
                        .withOptKeys(CONNECTED_DEVICES_FIELD)
                        .concat(baseGen);


        var peripheralSpec =
                JsSpecs.ofNamedSpec(PERIPHERAL_FIELD,
                                    oneSpecOf(mouseSpec,
                                              keyboardSpec,
                                              usbHubSpec));

        var peripheralGen =
                NamedGen.of(PERIPHERAL_FIELD,
                            Combinators.oneOf(mouseGen,
                                              keyboardGen,
                                              usbHubGen));

        var parser = JsObjSpecParser.of(peripheralSpec);

        Schema schema = SpecToSchema.convert(peripheralSpec);

        System.out.println(schema);

        SpecSerializer serializer =
                SpecSerializerBuilder.of(peripheralSpec)
                                     .enableDebug("person-serializer")
                                     .build();
        SpecDeserializer deserializer =
                SpecDeserializerBuilder.of(peripheralSpec, peripheralSpec)
                                       .enableDebug("person-deserializer")
                                       .build();

        peripheralGen.sample(10).peek(System.out::println)
                     .forEach(obj -> {


                                  byte[] serialized = serializer.binaryEncode(obj);

                                  JsObj a = deserializer.binaryDecode(serialized);

                                  Assertions.assertEquals(obj,a.filterValues(it->it.isNotNull()));


                              }
                             );


    }


}