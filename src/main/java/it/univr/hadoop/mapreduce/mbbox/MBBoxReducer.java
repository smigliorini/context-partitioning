package it.univr.hadoop.mapreduce.mbbox;

import it.univr.util.Stats;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

public class MBBoxReducer extends Reducer<Text, ObjectWritable, Writable, Writable> {
    static final Logger LOGGER = LogManager.getLogger(MBBoxReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
        Stats<WritableComparable> collect = StreamSupport.stream(values.spliterator(), false)
                .map(value -> (WritableComparable) value.get())
                .collect(Stats.collector(WritableComparable::compareTo));
        if(collect.count() > 0) {
            context.write(key, collect.min());
            context.write(key, collect.max());
        } else
            LOGGER.warn("No min max value found!");
    }
}
