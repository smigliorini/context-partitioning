package it.univr.hadoop;

import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.conf.PartitionTechnique;
import it.univr.hadoop.mapreduce.boxcount.BoxCountingMapper;
import it.univr.hadoop.mapreduce.boxcount.BoxCountingReducer;
import it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce;
import it.univr.hadoop.mapreduce.multidim.MultiDimMapper;
import it.univr.hadoop.mapreduce.multidim.MultiDimReducer;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridMapper;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridReducer;
import it.univr.hadoop.util.ContextBasedUtil;
import it.univr.util.ReflectionUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;


public class ContextBasedPartitioner {
    static final Logger LOGGER = LogManager.getLogger(ContextBasedPartitioner.class);
    public static long makePartitions(String[] args, Class<? extends FileInputFormat> inputFormatClass)
            throws IOException, ClassNotFoundException, InterruptedException {
        OperationConf config = new OperationConf(new GenericOptionsParser(args));
        if(!config.validInputOutputFiles()) {
            LOGGER.error("Invalid input files");
            System.exit(1);
        }


        //MBBox map reduce
        Pair<ContextData, ContextData> contextData = MBBoxMapReduce.runMBBoxMapReduce(config, inputFormatClass,
                false);
        //TODO Store the information as json string type inside the configuration parameter?
        ContextData minContextDataValue = contextData.getLeft();
        if(minContextDataValue != null) {
            //input setup
            OperationConf.setContextSetDim(config, contextData.getKey().getContextFields().length);
            //Width calculation
            Stream.of(minContextDataValue.getContextFields()).forEach(property -> {
                ContextData maxContextDataValue = contextData.getRight();
                Comparable<?> maxValue = (Comparable<?>) ReflectionUtil.readMethod(property, maxContextDataValue);
                Comparable<?> minValue = (Comparable<?>) ReflectionUtil.readMethod(property, minContextDataValue);
                Double max = Double.valueOf(maxValue.toString());
                Double min = Double.valueOf(minValue.toString());
                OperationConf.setMinProperty(config, property, min);
                OperationConf.setMaxProperty(config, property, max);
            });

            Job job = Job.getInstance(config, "CBMR");
            job.setJarByClass(ContextBasedPartitioner.class);
            //set Split size configuration

            config.hContextBasedConf.ifPresentOrElse(customConf -> {
                Long splitSize = customConf.getSplitSize(config.technique);
                FileInputFormat.setMinInputSplitSize(job, splitSize);
                FileInputFormat.setMaxInputSplitSize(job, splitSize);
            }, () -> {
                //mapreduce.input.fileinputformat.split.minsize
            });
            Path[] inputPaths = new Path[config.getFileInputPaths().size()];
            config.getFileInputPaths().toArray(inputPaths);
            FileInputFormat.setInputPaths(job, inputPaths);
            job.setInputFormatClass(inputFormatClass);

            //Mapper setup
            Class<?> mapOutputKeyClass = Text.class;//TODO
            Class<?> mapOutputValueClass = ContextData.class;
            Class<? extends Mapper> mapperClass;
            Class<? extends Reducer> reducerClass;
            if(config.technique == PartitionTechnique.ML_GRID) {
                mapperClass = MultiLevelGridMapper.class;
                reducerClass = MultiLevelGridReducer.class;
            } else if(config.technique == PartitionTechnique.MD_GRID) {
                mapperClass = MultiDimMapper.class;
                reducerClass = MultiDimReducer.class;
                Optional<Class<? extends ContextData>> present = ContextBasedUtil.getContextDataClassFromInputFormat(inputFormatClass);
                if(present.isPresent()) {
                    mapOutputValueClass = present.get();
                }
                //mapOutputKeyClass = LongWritable.class;
            } else {
                mapperClass = BoxCountingMapper.class;
                reducerClass = BoxCountingReducer.class;
            }
            job.setMapOutputKeyClass(mapOutputKeyClass);
            job.setMapOutputValueClass(mapOutputValueClass);
            job.setMapperClass(mapperClass);
            //Reducer setup
            job.setReducerClass(reducerClass);

            //output
            //TODO set a different format class?
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            //job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, config.getOutputPath());

            //job
            job.waitForCompletion(true);
            Counters counters = job.getCounters();
            Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
            return outputRecordCounter.getValue();
        }
        return 0;
    }
}
