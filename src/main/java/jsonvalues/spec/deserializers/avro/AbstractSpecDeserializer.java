package jsonvalues.spec.deserializers.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

abstract class AbstractSpecDeserializer extends AbstractDeserializer {

  final Schema schema;

  public AbstractSpecDeserializer(final Schema schema,
                                  final DecoderFactory decoderFactory,
                                  final BinaryDecoder reusedDecoder) {
    super(decoderFactory,
          reusedDecoder);
    this.schema = schema;

  }


}
