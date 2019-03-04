import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DistinctTest {

    private static class AnotherObjectCoder extends AtomicCoder<AnotherObject> {
        private static final AnotherObjectCoder INSTANCE = new AnotherObjectCoder();
        private StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();

        public static AnotherObjectCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(AnotherObject value, OutputStream outStream) throws CoderException, IOException {
            stringUtf8Coder.encode(value.getField(), outStream);
        }

        @Override
        public AnotherObject decode(InputStream inStream) throws CoderException, IOException {
            return new AnotherObject(stringUtf8Coder.decode(inStream));
        }
    }

    private static class AnotherObject {
        private String field;

        private AnotherObject(String field) {
            this.field = field;
        }

        private String getField() {
            return field;
        }
    }


    @Test
    public void test() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(SparkRunner.class);
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline
                .apply(Create.of(new AnotherObject("a")).withCoder(AnotherObjectCoder.of()))
                .apply(Distinct.<AnotherObject>create());

        pipeline.run().waitUntilFinish();
    }

}
