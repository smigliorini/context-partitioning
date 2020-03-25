package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static java.lang.String.format;

public class MBBoxMapper extends Mapper<LongWritable, ContextData, Text, ObjectWritable> {
    static final Logger LOGGER = LogManager.getLogger(MBBoxMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        LOGGER.info("MBBoxMapper setup");
    }

    @Override
    protected void map(LongWritable key, ContextData value, Context context) throws IOException, InterruptedException {
        for (String fieldName : value.getContextFields()) {
            try {
                Object propertyValue = new PropertyDescriptor(fieldName, value.getClass()).getReadMethod()
                        .invoke(value);
                if(propertyValue == null) {
                    LOGGER.error(format("Field name %s does not exist for record: %s",
                            fieldName, value.toString()));
                } else {
                    WritableComparable mapValue;
                    if(propertyValue instanceof WritableComparable) {
                        mapValue = (WritableComparable) propertyValue;
                    } else {
                        mapValue = WritablePrimitiveMapper.getPrimitiveWritable(propertyValue);
                    }
                    context.write(new Text(fieldName), new ObjectWritable(mapValue));
                }
            } catch (IllegalAccessException | IntrospectionException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

}