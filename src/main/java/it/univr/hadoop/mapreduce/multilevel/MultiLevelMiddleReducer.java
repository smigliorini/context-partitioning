package it.univr.hadoop.mapreduce.multilevel;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.mapreduce.MultiBaseReducer;
import it.univr.hadoop.util.ContextBasedUtil;
import it.univr.hadoop.writable.TextPairWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.Opt;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class MultiLevelMiddleReducer<VIN extends ContextData, VOUT extends ContextData> extends MultiBaseReducer<Text,
        VIN, VOUT> {

    private final Logger LOGGER = LogManager.getLogger(MultiLevelMiddleReducer.class);

    String propertyName;
    Optional<String> nextProperty;
    HashMap<Pair<String, String>, Pair<Double, Double>> minMax;

    public static final String MINMAX_FILE_NAME = "PropertyMinMax";

    @Override
    protected void setup(Context context) {
        super.setup(context);
        this.propertyName = OperationConf.getMultiLevelMapperProperty(context.getConfiguration());
        minMax = new HashMap<>();
        nextProperty = Optional.empty();
    }

    @Override
    protected void reduce(Text key, Iterable<VIN> values, Context context) throws IOException, InterruptedException {
        StreamSupport.stream(values.spliterator(), false).forEach(data -> {
            try {
                foreachOperation(key ,data);
                multipleOutputs.write(key, data, key.toString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                LOGGER.error(e.getMessage());
            }
        });
    }

    @Override
    protected void foreachOperation(Text key, VIN data) {
        if(nextProperty.isEmpty()) {
            List<String> strings = Arrays.asList(data.getContextFields());
            int i = strings.indexOf(propertyName);
            nextProperty = Optional.of(strings.get(i + 1));
        }
        Pair<String, String> hashKey = Pair.of(key.toString(), nextProperty.get());
        Pair<Double, Double> doubleDoublePair = minMax.get(hashKey);
        Double value = ContextBasedUtil.getDouble(nextProperty.get(), data);
        if(doubleDoublePair == null) {
            minMax.put(hashKey, Pair.of(value, value));
        } else {
            doubleDoublePair.getLeft();
            Double min = Math.min(doubleDoublePair.getLeft(), value);
            Double max = Math.max(doubleDoublePair.getRight(), value);
            minMax.put(hashKey, Pair.of(min, max));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(!nextProperty.isEmpty()) {
            for (Pair<String,String> key : minMax.keySet()) {
                Pair<Double, Double> doubleDoublePair = minMax.get(key);
                TextPairWritable valuePair = new TextPairWritable(doubleDoublePair.getLeft().toString(),
                        doubleDoublePair.getRight().toString());
                TextPairWritable keyPair = new TextPairWritable(key.getLeft(), key.getRight());
                multipleOutputs.write(keyPair, valuePair, MINMAX_FILE_NAME);
            }
        }
        super.cleanup(context);
    }
}
