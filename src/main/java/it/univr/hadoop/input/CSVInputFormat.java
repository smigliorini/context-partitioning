package it.univr.hadoop.input;


import it.univr.hadoop.ContextData;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public abstract  class CSVInputFormat<K, V extends ContextData> extends FileInputFormat<K ,V> {

}
