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

    String PARSE_RECORD_METHOD = "parseRecord";

    /**
     * Retrieve the context fields name, used by the partition technique. The order correspond to index order.
     */
    String[] getContextFields();

    /**
     * Read a line and parse It to ContextData Object
     * Method added cause of Multi-level grid partitioning needs to read raw-data from reducer text output format, made by
     * intermediate map-reduce for temporary files.
     * @param line
     * @return
     */
    ContextData parseRecord (String line);

}
