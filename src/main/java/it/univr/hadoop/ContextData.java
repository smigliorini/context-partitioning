package it.univr.hadoop;


import it.univr.veronacard.VeronaCardRecord;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * This class represents a Context data, It defines a part of the whole record represented by a subset of attributes.
 * Those attributes are part of the index partition.
 * The class does need get and set method for context fields.
 */
public interface ContextData extends Writable, WritableComparable<ContextData> {

    /**
     * Retrieve the context fields name, used by the partition technique. The order correspond to index order.
     */
    String[] getContextFields();

}
