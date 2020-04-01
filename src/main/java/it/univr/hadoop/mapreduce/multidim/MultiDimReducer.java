package it.univr.hadoop.mapreduce.multidim;

import it.univr.hadoop.ContextData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class MultiDimReducer extends Reducer<Text, ContextData, NullWritable, Text> {

    static final Logger LOGGER = LogManager.getLogger(MultiDimReducer.class);
    MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<ContextData> values, Context context) throws IOException, InterruptedException {
        StreamSupport.stream(values.spliterator(), false).forEach(data -> {
            try {
                multipleOutputs.write(NullWritable.get(), new Text(data.toString()), key.toString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
