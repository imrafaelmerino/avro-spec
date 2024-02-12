package jsonvalues.spec;

import java.util.function.Supplier;

class DebugUtils {

  static boolean debugNonNull(Object object) {
      if (object != null) {
          synchronized (System.out) {
              System.out.println(object);
          }
      }
    return object != null;
  }

  static boolean debugNonNull(Object object,
                              Supplier<String> message) {
      if (object != null) {
          synchronized (System.out) {
              System.out.println(message.get());
          }
      }
    return object != null;
  }
}
