package it.univr.hadoop.util;

import org.apache.hadoop.io.*;

public class WritablePrimitiveMapper {

    public static WritableComparable getPrimitiveWritable(Object value) {
        if(value instanceof String)
            return new Text((String) value);
        if(value instanceof Boolean)
            return new BooleanWritable((Boolean) value);
        if (value instanceof Integer)
            return new IntWritable((Integer) value);
        if(value instanceof Short)
            return new ShortWritable((Short) value);
        if(value instanceof Long)
            return new LongWritable((Long) value);
        if(value instanceof Float)
            return new FloatWritable((Float) value);
        if(value instanceof  Double)
            return new DoubleWritable((Double) value);

        return null;
    }

    public static Object getBeanFromWritable(WritableComparable value) {
        if (value instanceof Text)
            return value.toString();
        if(value instanceof BooleanWritable)
            return Boolean.valueOf(((BooleanWritable) value).get());
        if(value instanceof IntWritable)
            return ((IntWritable) value).get();
        if(value instanceof ShortWritable)
            return ((ShortWritable) value).get();
        if(value instanceof LongWritable)
            return ((LongWritable) value).get();
        if(value instanceof FloatWritable)
            return ((FloatWritable) value).get();
        if(value instanceof DoubleWritable)
            return ((DoubleWritable) value).get();
        return null;
    }

    public static Object getBeanObjectFromText(Text value, Class clazz) {
        if(clazz.isAssignableFrom(Boolean.class))
            return Boolean.valueOf(value.toString());
        if (clazz.isAssignableFrom(Integer.class))
            return Integer.valueOf(value.toString());
        if(clazz.isAssignableFrom(Short.class))
            return Short.valueOf(value.toString());
        if(clazz.isAssignableFrom(Long.class))
            return Long.valueOf(value.toString());
        if(clazz.isAssignableFrom(Float.class))
            return Float.valueOf(value.toString());
        if(clazz.isAssignableFrom(Double.class) )
            return Double.valueOf(value.toString());
        return null;
    }


}
