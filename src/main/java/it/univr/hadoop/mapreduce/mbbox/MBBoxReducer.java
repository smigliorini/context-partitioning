package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MBBoxReducer extends Reducer<Text, WritableComparable, Writable, Writable> {
    static final Logger LOGGER = LogManager.getLogger(MBBoxReducer.class);


    public class MBBoxCombiner extends Reducer<Text, WritableComparable, Text, WritableComparable> {
        @Override
        protected void reduce(Text key, Iterable<WritableComparable> values, Context context) throws IOException, InterruptedException {
            Optional<WritableComparable> max = StreamSupport.stream(values.spliterator(), false)
                    .max(WritableComparable::compareTo);
            if(max.isPresent())
                context.write(key, max.get());
            else
                LOGGER.warn("No max value found!");
        }
    }

    @Override
    protected void reduce(Text key, Iterable<WritableComparable> values, Context context) throws IOException, InterruptedException {
        OperationConf conf = (OperationConf) context.getConfiguration();
        ContextData mbrContextData = conf.getMbrContextData();
        Optional<WritableComparable> max = StreamSupport.stream(values.spliterator(), false).max(WritableComparable::compareTo);
        if(max.isPresent()) {
            WritableComparable writableComparable = max.get();
            try {
                PropertyDescriptor descriptor = new PropertyDescriptor(key.toString(), mbrContextData.getClass());
                Object value = writableComparable;
                if(!WritableComparable.class.isAssignableFrom(descriptor.getPropertyEditorClass()))
                    value = WritablePrimitiveMapper.getBeanFromWritable(writableComparable);
                descriptor.getWriteMethod().invoke(value);
            } catch (IntrospectionException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        context.write(NullWritable.get(), NullWritable.get());
    }
}
