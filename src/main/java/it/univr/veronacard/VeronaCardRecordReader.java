package it.univr.veronacard;

import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.input.FileRecordReader;
import it.univr.partitioning.DataUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class VeronaCardRecordReader<V extends VeronaCardWritable> extends FileRecordReader<V> {
    static final Logger LOGGER = LogManager.getLogger(VeronaCardRecordReader.class);

    @Override
    protected Pair<LongWritable, V> parseLine(String line) {
        VeronaCardWritable record = new VeronaCardWritable(DataUtils.parseRecord(line, FileRecordReader.DEFAULT_SEPARATOR));
        LongWritable key = new LongWritable(reader.getCurrentKey().get());
        return new Pair(key, record);
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
    }
}
