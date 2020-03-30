package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MBBoxReducer extends Reducer<Text, ObjectWritable, Writable, Writable> {
    static final Logger LOGGER = LogManager.getLogger(MBBoxReducer.class);

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
            context.write(key, min);
        }
        if(max != null) {
            context.write(key, max);
        }
    }
}
