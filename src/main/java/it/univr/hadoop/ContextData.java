package it.univr.hadoop;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * This class represents a Context data, It defines a part of the whole record represented by a subset of attributes.
 * Those attributes are part of the index partition.
 */
public interface ContextData extends Writable, Cloneable, WritableComparable<ContextData> {

    /**
     * Retrieve the context fields name, used by the partition technique. The order correspond to index order.
     */
    String[] getContextFields();
}
