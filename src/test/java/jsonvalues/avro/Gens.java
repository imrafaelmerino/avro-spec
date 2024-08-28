package jsonvalues.avro;

import static io.confluent.kafka.schemaregistry.avro.AvroSchema.NAME_FIELD;
import static jsonvalues.avro.Fields.BUTTON_COUNT_FIELD;
import static jsonvalues.avro.Fields.CONNECTED_DEVICES_FIELD;
import static jsonvalues.avro.Fields.KEY_COUNT_FIELD;
import static jsonvalues.avro.Fields.MEDIA_BUTTONS_FIELD;
import static jsonvalues.avro.Fields.PERIPHERAL_FIELD;
import static jsonvalues.avro.Fields.TRACKING_TYPE_ENUM;
import static jsonvalues.avro.Fields.TRACKING_TYPE_FIELD;
import static jsonvalues.avro.Fields.TYPE_FIELD;
import static jsonvalues.avro.Fields.WHEEL_COUNT_FIELD;

import fun.gen.Combinators;
import fun.gen.Gen;
import fun.gen.NamedGen;
import jsonvalues.JsObj;
import jsonvalues.JsStr;
import jsonvalues.gen.JsArrayGen;
import jsonvalues.gen.JsBoolGen;
import jsonvalues.gen.JsIntGen;
import jsonvalues.gen.JsObjGen;
import jsonvalues.gen.JsStrGen;

public class Gens {

  public static final JsObjGen baseGen =
      JsObjGen.of(NAME_FIELD,
                  JsStrGen.alphabetic());

  public static final JsObjGen mouseGen =
      JsObjGen.of(BUTTON_COUNT_FIELD,
                  JsIntGen.arbitrary(0,
                                     10),
                  WHEEL_COUNT_FIELD,
                  JsIntGen.arbitrary(0,
                                     10),
                  TRACKING_TYPE_FIELD,
                  Combinators.oneOf(TRACKING_TYPE_ENUM)
                             .map(JsStr::of),
                  TYPE_FIELD,
                  Gen.cons(JsStr.of("mouse"))
                 )
              .concat(baseGen);

  public static final JsObjGen keyboardGen =

      JsObjGen.of(KEY_COUNT_FIELD,
                  JsIntGen.arbitrary(0,
                                     10),
                  MEDIA_BUTTONS_FIELD,
                  JsBoolGen.arbitrary(),
                  TYPE_FIELD,
                  Gen.cons(JsStr.of("keyboard"))
                 )
              .concat(baseGen);

  public static final JsObjGen usbHubGen =
      JsObjGen.of(CONNECTED_DEVICES_FIELD,
                  JsArrayGen.biased(NamedGen.of(PERIPHERAL_FIELD),
                                    2,
                                    10),
                  TYPE_FIELD,
                  Gen.cons(JsStr.of("usb_hub"))
                 )
              .withOptKeys(CONNECTED_DEVICES_FIELD)
              .concat(baseGen);


  public static final Gen<JsObj> peripheralGen =
      NamedGen.of(PERIPHERAL_FIELD,
                  Combinators.oneOf(mouseGen,
                                    keyboardGen,
                                    usbHubGen));


}
