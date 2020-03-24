package it.univr.hadoop.mapreduce.mbbox;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;


public class MBBoxCombiner extends Reducer<Text, ObjectWritable, Text, ObjectWritable> {

    static final Logger LOGGER_COMBINER = LogManager.getLogger(MBBoxCombiner.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        LOGGER_COMBINER.info("MBBoxCombiner setup");
    }

    @Override
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
        Optional<WritableComparable> max = StreamSupport.stream(values.spliterator(), false)
                .map(value -> (WritableComparable) value.get())
                .max(WritableComparable::compareTo);
        if(max.isPresent())
            context.write(key, new ObjectWritable(max.get()));
        else
            LOGGER_COMBINER.warn("No max value found!");
    }
}
