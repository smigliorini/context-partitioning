package it.univr.veronacard;

import it.univr.hadoop.input.CSVRecordReader;
import it.univr.partitioning.DataUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;

public class VeronaCardRecordReader<V extends VeronaCardWritable> extends CSVRecordReader<Text, V> {

    @Override
    protected Pair<Text, V> parseLine(String line) {
        VeronaCard record = DataUtils.parseRecord(line, CSVRecordReader.DEFAULT_SEPARATOR);
        return null;
    }
}
