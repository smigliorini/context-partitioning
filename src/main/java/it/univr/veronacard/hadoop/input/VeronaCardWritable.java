package it.univr.veronacard.hadoop.input;

import org.apache.hadoop.io.Text;

public class VeronaCardWritable extends CSVWritable {
    @Override
    public Text toText(Text text) {
        return null;
    }

    @Override
    public void fromText(Text text) {

    }
}
