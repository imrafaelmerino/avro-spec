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
import static jsonvalues.spec.JsSpecs.oneSpecOf;

import java.util.Map;
import jsonvalues.JsNull;
import jsonvalues.spec.JsArraySpec;
import jsonvalues.spec.JsEnumBuilder;
import jsonvalues.spec.JsObjSpec;
import jsonvalues.spec.JsObjSpecBuilder;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.JsSpecs;
import jsonvalues.spec.SpecToAvroSchema;

public class Specs {

  public static final JsObjSpec baseSpec =
      JsObjSpec.of(NAME_FIELD,
                   JsSpecs.str(),
                   TYPE_FIELD,
                   JsEnumBuilder.withName("type")
                                .build("mouse",
                                       "keyboard",
                                       "usb_hub"));


  public static final JsObjSpec mouseSpec =
      JsObjSpecBuilder.withName("mouse")
                      .build(JsObjSpec.of(BUTTON_COUNT_FIELD,
                                          JsSpecs.integer(),
                                          WHEEL_COUNT_FIELD,
                                          JsSpecs.integer(),
                                          TRACKING_TYPE_FIELD,
                                          JsEnumBuilder.withName("tracking_type")
                                                       .build(TRACKING_TYPE_ENUM)
                                         ))
                      .concat(baseSpec);


  public static final JsObjSpec keyboardSpec =
      JsObjSpecBuilder.withName("keyboard")
                      .build(JsObjSpec.of(KEY_COUNT_FIELD,
                                          JsSpecs.integer(),
                                          MEDIA_BUTTONS_FIELD,
                                          JsSpecs.bool()
                                         ))
                      .concat(baseSpec);


  public static final JsObjSpec usbHubSpec =
      JsObjSpecBuilder.withName("usb_hub")
                      .withFieldsDefaults(Map.of(CONNECTED_DEVICES_FIELD,
                                                 JsNull.NULL))
                      .build(JsObjSpec.of(CONNECTED_DEVICES_FIELD,
                                          JsSpecs.arrayOfSpec(JsSpecs.ofNamedSpec(PERIPHERAL_FIELD))
                                                 .nullable()
                                         )
                                      .withOptKeys(CONNECTED_DEVICES_FIELD)
                                      .concat(baseSpec));


  public static final JsSpec peripheralSpec =
      JsSpecs.ofNamedSpec(PERIPHERAL_FIELD,
                          oneSpecOf(mouseSpec,
                                    keyboardSpec,
                                    usbHubSpec
                                   )
                         );



  public static JsObjSpec paymentSpec =
      JsObjSpecBuilder.withName("Payment")
                      .withNamespace("io.confluent.examples.clients.basicavro")
                      .build(JsObjSpec.of("id",
                                          JsSpecs.str(),
                                          "amount",
                                          JsSpecs.doubleNumber()
                                         )
                            );

  public static JsArraySpec arrayPaymentSpec = JsSpecs.arrayOfSpec(paymentSpec);

  public static void main(String[] args) {
    System.out.println(SpecToAvroSchema.convert(arrayPaymentSpec));
    System.out.println(SpecToAvroSchema.convert(peripheralSpec));
  }
}
