package it.univr.veronacard;

import it.univr.hadoop.input.CSVRecordReader;
import it.univr.partitioning.DataUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;

public class VeronaCardRecordReader<V extends VeronaCardWritable> extends CSVRecordReader<LongWritable, V> {

    @Override
    protected Pair<LongWritable, V> parseLine(String line) {
        VeronaCardWritable record = new VeronaCardWritable(DataUtils.parseRecord(line, CSVRecordReader.DEFAULT_SEPARATOR));
        LongWritable key = new LongWritable(pos);
        return new Pair(key, record);
    }
}
