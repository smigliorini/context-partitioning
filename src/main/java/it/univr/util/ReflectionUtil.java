package it.univr.util;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;

public class ReflectionUtil {

    final static Logger LOGGER = LogManager.getLogger(ReflectionUtil.class);


    public static Object readMethod(String property, Object bean) {
        try {
            return PropertyUtils.getProperty(bean, property);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            LOGGER.error("Type, field mismatch due to reflection");
            e.printStackTrace();
        }
        return null;
    }
}
