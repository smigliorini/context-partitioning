package it.univr.veronacard;

import it.univr.hadoop.ContextData;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VeronaCardWritable extends VeronaCard implements ContextData {

    @Override
    public String[] getContextFields() {
        return new String[]{"time", "x", "y", "age"};
    }

    @Override
    public int compareTo(ContextData o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public Text toText(Text text) {
        return null;
    }

    @Override
    public void fromText(Text text) {

    }


}
