package it.univr.hadoop.input;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;


public abstract class CSVRecordReader<K, V> extends RecordReader<K, V> {

    public static final String DEFAULT_DELIMITER = "\"";
    public static final String DEFAULT_SEPARATOR = ",";

    protected K key;
    protected V value;

    //Configuration conf;
    //InputStream inputStream;
    protected LineReader lineReader;
    protected String line;
    protected long end;
    protected long start;
    protected long pos;

    public CSVRecordReader() {
        super();
    }

    public CSVRecordReader(InputStream is) throws IOException {
        init(is);
    }

    protected void init(InputStream is) {
        this.lineReader = new LineReader(is);
    }


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        Path path = fileSplit.getPath();
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(path);
        open.seek(start);
        InputStream inputStream = open;
        this.pos = start;
        init(inputStream);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Text text = new Text();
        int size = lineReader.readLine(text);
        if(size >= 0) {
            Pair<K, V> tuple = parseLine(text.toString());
            key = tuple.getKey();
            value = tuple.getValue();
            pos += size;
            return true;
        }
        else
            return false;
    }

    protected abstract Pair<K, V> parseLine(String line);


    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start == end )
            return 0f;
        return Math.min(1.0f, (pos - start) / (float) (end - start));
    }

    @Override
    public synchronized void close() throws IOException {
        if(lineReader != null) {
            lineReader.close();
            lineReader = null;
        }
    }
}
