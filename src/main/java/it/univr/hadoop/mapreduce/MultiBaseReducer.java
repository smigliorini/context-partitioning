package it.univr.hadoop.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.StreamSupport;

public abstract class MultiBaseReducer<KEYIN extends WritableComparable, VIN extends Writable, VOUT extends Writable>
        extends Reducer<KEYIN, VIN, NullWritable, VOUT> {

    private static final Logger BASE_LOGGER = LogManager.getLogger(MultiBaseReducer.class);
    protected MultipleOutputs<Writable, Writable> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(KEYIN key, Iterable<VIN> values, Context context) throws IOException, InterruptedException {

        StreamSupport.stream(values.spliterator(), false).forEach(data -> {
            try {
                foreachOperation(key ,data);
                multipleOutputs.write(NullWritable.get(), data, key.toString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                BASE_LOGGER.warn(e.getMessage());
            }
        });
    }

    protected void foreachOperation(KEYIN key, VIN data) {}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
