package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class MBBoxMapper extends Mapper<LongWritable, ContextData, Text, WritableComparable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, ContextData value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        for (String fieldName : value.getContextFields()) {
            try {
                Object propertyValue = new PropertyDescriptor(fieldName, value.getClass()).getReadMethod().invoke(value);
                WritableComparable mapValue;
                if(value instanceof WritableComparable) {
                    mapValue = value;
                } else {
                    mapValue = WritablePrimitiveMapper.getPrimitiveWritable(propertyValue);
                }
                context.write(new Text(fieldName), mapValue);
            } catch (IllegalAccessException | IntrospectionException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}