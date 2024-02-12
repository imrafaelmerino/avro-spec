package jsonvalues.spec;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Avro Spec Serializer Event")
@Name("AvroSpecSerializerEvent")
@Category({"Avro Spec Serializers Events"})
@Description("Event for tracking Avro spec serialization")
class SpecSerializerEvent extends Event {

  private static final AtomicLong successCounter = new AtomicLong(0);
  private static final AtomicLong errorCounter = new AtomicLong(0);
  @Label("Serializer Name")
  final String name;
  /**
   * Either SUCCESS OR FAILURE
   */
  @Label("Event Result")
  public String result;
  /**
   * The exception details in case of failure
   */
  @Label("Exception Details")
  String exceptionDetails = "";
  @Label("Success Counter")
  long successCounterStat;
  @Label("Error Counter")
  long errorsCountersStat;

  SpecSerializerEvent(String name) {
    this.name = Objects.requireNonNull(name);
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
