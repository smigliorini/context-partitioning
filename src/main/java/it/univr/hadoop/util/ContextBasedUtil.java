package it.univr.hadoop.util;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.writable.TextPairWritable;
import it.univr.util.ReflectionUtil;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.swing.text.html.Option;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;

public class ContextBasedUtil {
    final static Logger LOGGER = LogManager.getLogger(ContextBasedUtil.class);

    public static Optional<? extends ContextData> getContextDataInstanceFromInputFormat(Class< ? extends FileInputFormat> inputFormatClass) {
        //Retrieve InputFormat information to process the data and return a Context data value holder of the range.
        Type genericSuperclass = inputFormatClass.getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
        Type[] actualTypeArgument = parameterizedType.getActualTypeArguments();
        Type type = actualTypeArgument[1];
        if (ContextData.class.isAssignableFrom((Class<?>) type)) {
            Class<? extends ContextData> valueClass = (Class<? extends ContextData>) type;
            try {
                return Optional.of(valueClass.getConstructor().newInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    public static Optional<Class<? extends ContextData>> getContextDataClassFromInputFormat(Class< ? extends FileInputFormat> inputFormatClass) {
        //Retrieve InputFormat information to process the data and return a Context data value holder of the range.
        Type genericSuperclass = inputFormatClass.getGenericSuperclass();
        ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
        Type[] actualTypeArgument = parameterizedType.getActualTypeArguments();
        Type type = actualTypeArgument[1];
        if (ContextData.class.isAssignableFrom((Class<?>) type)) {
            return Optional.of((Class<? extends ContextData>) type);
        }
        return Optional.empty();
    }

    public static Double getDouble(String propertyName, Object data) {
        Object propertyValue = ReflectionUtil.readMethod(propertyName, data);
        Double value;
        if (propertyValue instanceof WritableComparable)
            value = Double.valueOf(WritablePrimitiveMapper
                    .getBeanFromWritable((WritableComparable) propertyValue).toString());
        else
            value = Double.valueOf(propertyValue.toString());
        return value;

    }


}
