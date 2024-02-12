package jsonvalues.spec;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Avro Spec Deserializer Event")
@Name("AvroSpecDeserializerEvent")
@Category({"Avro Spec Deserializer Events"})
@Description("Event for tracking Avro spec deserialization")
class SpecDeserializerEvent extends Event {

  private static final AtomicLong successCounter = new AtomicLong(0);
  private static final AtomicLong errorCounter = new AtomicLong(0);
  @Label("Deserializer Name")
  final String name;
  @Label("Success Counter")
  long successCounterStat = 0;
  @Label("Error Counter")
  long errorsCountersStat = 0;
  /**
   * Either SUCCESS OR FAILURE
   */
  @Label("Event Result")
  String result;
  /**
   * The exception details in case of failure
   */
  @Label("Exception Details")
  String exceptionDetails = "";

  SpecDeserializerEvent(String name) {
    this.name = requireNonNull(name);
  }

  void registerSuccess() {
    result = RESULT.SUCCESS.name();
    successCounterStat = successCounter.incrementAndGet();
  }

  void registerError(Exception e) {
    exceptionDetails = "%s: %s".formatted(e.getClass(),
                                          e.getMessage());
    result = RESULT.FAILURE.name();
    errorsCountersStat = errorCounter.incrementAndGet();
  }


  private enum RESULT {SUCCESS, FAILURE}

}
