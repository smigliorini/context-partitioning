package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.ContextBasedReducer;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import it.univr.hadoop.util.ContextBasedUtil;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.hadoop.writable.TextPairWritable;
import it.univr.util.ReflectionUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MultiLevelGridReducer<VIN extends ContextData, VOUT extends ContextData> extends ContextBasedReducer<Text,
        VIN, VOUT> {

    private final Logger LOGGER = LogManager.getLogger(MultiLevelGridReducer.class);

    @Override
    protected void reduce(Text key, Iterable<VIN> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
