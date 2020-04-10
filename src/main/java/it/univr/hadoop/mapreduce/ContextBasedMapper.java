package it.univr.hadoop.mapreduce;

import it.univr.hadoop.ContextData;
import org.apache.hadoop.io.WritableComparable;

public abstract class ContextBasedMapper <KEYIN, VALUEIN ,
        KEYOUT extends WritableComparable, VOUT extends ContextData> extends MultiBaseMapper<KEYIN, VALUEIN, KEYOUT, VOUT> {

}
