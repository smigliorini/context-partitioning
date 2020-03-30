package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.util.Stats;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.util.Iterator;
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
        //TODO make the min max calculation with Java stream and parallelism
        Iterator<ObjectWritable> iterator = values.iterator();

        WritableComparable min = null;
        WritableComparable max = null;
        while (iterator.hasNext()) {
            if(min == null) {
                min = (WritableComparable) iterator.next().get();
                max = (WritableComparable) iterator.next().get();
            } else {
                WritableComparable next = (WritableComparable) iterator.next().get();
                if(max.compareTo(next) < 0)
                    max = next;
                if(min.compareTo(next) > 0)
                    min = next;
            }
        }

        if(min != null) {
            context.write(key, new ObjectWritable(min));
        }else
            LOGGER_COMBINER.warn("No min value found!");
        if(min != null) {
            context.write(key, new ObjectWritable(max));
        }else
            LOGGER_COMBINER.warn("No max value found!");
    }
}
