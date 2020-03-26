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
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MBBoxReducer extends Reducer<Text, ObjectWritable, Writable, Writable> {
    static final Logger LOGGER = LogManager.getLogger(MBBoxReducer.class);

    @Override
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {

        Optional<WritableComparable> max = StreamSupport.stream(values.spliterator(), false)
                .map(value -> (WritableComparable) value.get())
                .max(WritableComparable::compareTo);


        if(max.isPresent()) {
            context.write(key, max.get());
        }
    }
}
