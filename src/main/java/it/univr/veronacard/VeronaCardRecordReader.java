package it.univr.veronacard;

import it.univr.hadoop.input.CSVRecordReader;
import it.univr.hadoop.mapreduce.mbbox.MBBoxMapper;
import it.univr.partitioning.DataUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static java.lang.String.format;

public class VeronaCardRecordReader<V extends VeronaCardWritable> extends CSVRecordReader<LongWritable, V> {
    static final Logger LOGGER = LogManager.getLogger(VeronaCardRecordReader.class);

    @Override
    protected Pair<LongWritable, V> parseLine(String line) {
        VeronaCardWritable record = new VeronaCardWritable(DataUtils.parseRecord(line, CSVRecordReader.DEFAULT_SEPARATOR));
        LongWritable key = new LongWritable(pos);
        //LOGGER.info(format("Key: %d, record: %s, sting line: %s", key.get(), record.toString(), line));
        return new Pair(key, record);
    }
}
