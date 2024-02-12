package jsonvalues.spec;

import org.apache.avro.Schema;

class AvroUtils {

  static boolean isRecordSchema(Schema schema) {
    return schema.getType() == Schema.Type.RECORD
           || (schema.isUnion()
               && schema.getTypes()
                        .stream()
                        .allMatch(it -> it.getType() == Schema.Type.RECORD)
           );
  }


}
