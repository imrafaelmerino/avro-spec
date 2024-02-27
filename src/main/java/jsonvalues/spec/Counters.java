package jsonvalues.spec;

import java.util.concurrent.atomic.AtomicLong;

final class Counters {

  static AtomicLong serializerCounter = new AtomicLong(0);
  static AtomicLong deserializerCounter = new AtomicLong(0);
}
