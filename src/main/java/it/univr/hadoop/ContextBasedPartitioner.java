package it.univr.hadoop;

import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.conf.PartitionTechnique;
import it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce;
import it.univr.hadoop.mapreduce.multidim.MultiDimMapper;
import it.univr.hadoop.mapreduce.multidim.MultiDimReducer;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridMapper;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridReducer;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelItemChainMapper;
import it.univr.hadoop.util.ContextBasedUtil;
import it.univr.hadoop.writable.TextPairWritable;
import it.univr.util.ReflectionUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
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

    String[] args;
    Class<? extends FileInputFormat> inputFormatClass;

    public ContextBasedPartitioner(String[] args, Class<? extends FileInputFormat> inputFormatClass) {
        this.args = args;
        this.inputFormatClass = inputFormatClass;
    }

    public long runPartitioner()
            throws IOException, ClassNotFoundException, InterruptedException {
        //LogManager.getRootLogger().setLevel(Level.WARN);
        OperationConf config = new OperationConf(new GenericOptionsParser(args));
        if(!config.validInputOutputFiles()) {
            LOGGER.error("Invalid input files");
            System.exit(1);
        }
        //Retrieve Minimum Bounding Box Map-Reduce result
        Pair<ContextData, ContextData> contextData = MBBoxMapReduce.runMBBoxMapReduce(config, inputFormatClass,
                false);
        if(contextData != null) {
            //TODO Passing a json string as parameter configuration could be another way to access to needed information from Reducer and Mappers.
            String[] contextFields = contextData.getLeft().getContextFields();
            Job job = getJob(contextData.getLeft(), contextData.getRight(), config);
            //Mapper setup
            if(config.technique == PartitionTechnique.MD_GRID || config.technique == PartitionTechnique.BOX_COUNT) {
                if(config.technique == PartitionTechnique.MD_GRID){
                    configureMultiDimPartitioner(job);
                } else {
                    configureBoxCountPartitioner(job);
                }
            } else {
                //PartitionTechnique.ML_GRID
                configureMultiLevelPartitioner(job, config, contextFields.length);
            }

            //output
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, config.getOutputPath());

            //job
            job.waitForCompletion(true);
            Counters counters = job.getCounters();
            Counter outputRecordCounter = counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
            return outputRecordCounter.getValue();
        }
        LOGGER.warn("No found data");
        return 0;
    }

    private Job getJob(ContextData minContextDataValue, ContextData maxContextDataValue, OperationConf config) throws IOException {
        //TODO Passing a json string as parameter configuration could be another way to access to needed information from Reducer and Mappers.
        String[] contextFields = minContextDataValue.getContextFields();
        //input setup
        OperationConf.setContextSetDim(config, contextFields.length);
        //Width calculation
        Stream.of(contextFields).forEach(property -> {
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
        return job;
    }

    private void configureMultiDimPartitioner (Job job) {
        Class<?> mapOutputValueClass = ContextData.class;
        Optional<Class<? extends ContextData>> present = ContextBasedUtil.getContextDataClassFromInputFormat(inputFormatClass);
        if(present.isPresent()) {
            mapOutputValueClass = present.get();
        }
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(mapOutputValueClass);
        job.setMapperClass(MultiDimMapper.class);
        //Reducer
        job.setReducerClass(MultiDimReducer.class);
    }

    private void configureMultiLevelPartitioner(Job job, OperationConf config, int contextFields) throws IOException {
        //PartitionTechnique.ML_GRID
        Optional<Class<? extends ContextData>> present = ContextBasedUtil.getContextDataClassFromInputFormat(inputFormatClass);

        ChainMapper.addMapper(job, MultiLevelGridMapper.class, LongWritable.class, present.get(),
                TextPairWritable.class, present.get(), config);
        //set intermediate mappers
        for(int i=1; i < contextFields; i++) {
            ChainMapper.addMapper(job, MultiLevelItemChainMapper.class, TextPairWritable.class, present.get(),
                    TextPairWritable.class, present.get(), config);
        }

        ChainReducer.setReducer(job, MultiLevelGridReducer.class, TextPairWritable.class, present.get(),
                NullWritable.class, present.get(), config);
    }

    private void configureBoxCountPartitioner(Job job) {
    }


}
