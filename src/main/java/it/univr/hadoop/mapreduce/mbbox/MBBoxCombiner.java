package it.univr.hadoop.mapreduce.mbbox;

import it.univr.util.Stats;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.StreamSupport;

import static java.lang.String.format;


public class MBBoxCombiner extends Reducer<Text, ObjectWritable, Text, ObjectWritable> {

    static final Logger LOGGER_COMBINER = LogManager.getLogger(MBBoxCombiner.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        LOGGER_COMBINER.info("MBBoxCombiner setup");
    }

    @Override
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
        Stats<WritableComparable> collect = StreamSupport.stream(values.spliterator(), false)
                .map(value -> (WritableComparable) value.get())
                .collect(Stats.collector(WritableComparable::compareTo));
        if(collect.count() > 0){
            context.write(key, new ObjectWritable(collect.min()));
            context.write(key, new ObjectWritable(collect.max()));
        } else
            LOGGER_COMBINER.warn("No min max value found!");
    }
}
