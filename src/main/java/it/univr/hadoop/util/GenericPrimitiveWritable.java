package it.univr.hadoop.util;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class GenericPrimitiveWritable extends GenericWritable {

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return new Class[0];
    }
}
