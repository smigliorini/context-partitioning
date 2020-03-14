package it.univr.hadoop;


import it.univr.hadoop.input.TextSerializable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * This class represents a Context data, It defines a part of the whole record represented by a subset of attributes.
 * Those attributes are part of the index partition.
 */
public interface ContextData extends Writable, Cloneable, TextSerializable, WritableComparable<ContextData> {

    /**
     * Retrieve the context fields name, used as by the partition technique.
     */
    String[] getContextFields();
}
