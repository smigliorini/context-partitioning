package it.univr.hadoop.mapreduce;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.mapreduce.multidim.MultiDimReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.StreamSupport;

public abstract class MultiBaseReducer<K extends WritableComparable, V extends ContextData> extends Reducer<K, V, NullWritable, Text> {

    private static final Logger BASE_LOGGER = LogManager.getLogger(MultiBaseReducer.class);
    protected MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(K key, Iterable<V> values, Context context) throws IOException, InterruptedException {

        StreamSupport.stream(values.spliterator(), false).forEach(data -> {
            try {
                multipleOutputs.write(NullWritable.get(), new Text(data.toString()), key.toString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                BASE_LOGGER.warn(e.getMessage());
            }
        });
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
