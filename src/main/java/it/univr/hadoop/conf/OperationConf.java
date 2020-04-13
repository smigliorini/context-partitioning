package it.univr.hadoop.conf;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class OperationConf extends Configuration {
    public static final Logger LOGGER = LogManager.getLogger(OperationConf.class);
    public static final String CELL_SIDE_PROPERTY = "cell-side";
    public static final String SPLIT_NUMBER_FILES = "split-number-files";
    public static final String CONTEXT_SET_DIM = "ctx-set-dim";
    public static final String MIN_PROPERTY_FIELD = "Min";
    public static final String MAX_PROPERTY_FIELD = "Max";
    public static final String MULTI_LEVEL_MAPPER_PROPERTY_NAME = "multi-level-mapper-property-name";
    public static final String MULTI_LEVEL_PARSER_METHOD_NAME = "multi-level-parse-method-name";
    public static final String MULTI_LEVEL_PARSER_CLASS = "multi-level-parse-class";
    public static final String MULTI_LEVEL_OUTPUT_PATH = "multi-level-outputpath";

    public Optional<HContexBasedConf> hContextBasedConf;
    public Vector<Path> fileInputPaths;
    public Path outputPath;
    public PartitionTechnique technique;


    public OperationConf(GenericOptionsParser genericOptionsParser) {
        super(genericOptionsParser.getConfiguration());
        initConfiguration(genericOptionsParser.getRemainingArgs());
    }
    private void initConfiguration(String ... args) {
        JAXBContext jaxbContext;
        try {
            jaxbContext = JAXBContext.newInstance(HContexBasedConf.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            hContextBasedConf = Optional.of((HContexBasedConf) unmarshaller.unmarshal(getClass().getClassLoader()
                    .getResource("conf.xml")));
        } catch (JAXBException e) {
            LOGGER.error("Can't load Hadoop Context based configuration");
            e.printStackTrace();
        }

        fileInputPaths = new Vector(Stream.of(args)
                .filter(s -> s.contains("/") || s.contains("\\"))
                .map(s -> new Path(s))
                .collect(Collectors.toList()));
        if(fileInputPaths.size() > 1) {
            outputPath = fileInputPaths.get(fileInputPaths.size() - 1);
            fileInputPaths.remove(fileInputPaths.size() - 1);
        }

        technique = Stream.of(args).filter(arg -> arg.contains(PartitionTechnique.BOX_COUNT.getPartitionTechnique())
                || arg.contains(PartitionTechnique.MD_GRID.getPartitionTechnique())
                || arg.contains(PartitionTechnique.ML_GRID.getPartitionTechnique())).map(s -> PartitionTechnique.valueOf(s))
                .findFirst().orElse(PartitionTechnique.MD_GRID);
    }

    /**
     * @return Return Validation of files. In case of the no output directory a default will be added in the same
     * directory of the first input file
     */
    public boolean validInputOutputFiles() throws IOException {
        if(fileInputPaths.size() == 0)
            return false;
        for(Path path : fileInputPaths) {
            if(!path.getFileSystem(this).exists(path))
                return false;
        }
        if(outputPath != null) {
            if(outputPath.getFileSystem(this).exists(outputPath)) {
                LOGGER.error("Output directory does already exist");
                return false;
            }
        } else {
            if(outputPath == null) {
                Path path = fileInputPaths.get(0);
                outputPath = new Path(path.getParent().toString()+File.separator +"out");
                if(outputPath.getFileSystem(this).exists(outputPath)) {
                    LOGGER.error("Output directory does already exist");
                    return false;
                }
            }
        }

        return true;
    }

    public Optional<HContexBasedConf> getHContextBasedConf() {
        return hContextBasedConf;
    }

    public Vector<Path> getFileInputPaths() {
        return fileInputPaths;
    }

    public void setFileInputPaths(Vector<Path> fileInputPaths) {
        this.fileInputPaths = fileInputPaths;
    }


    public Path getOutputPath() {
        return outputPath;
    }

    public void setOutputDirectory(Path outputPath) {
        this.outputPath = outputPath;
    }

    public PartitionTechnique getTechnique() {
        return technique;
    }

    public static void setSplitNumberFiles(Configuration conf, int splitNumberFiles) {
        conf.set(SPLIT_NUMBER_FILES, String.valueOf(splitNumberFiles));
    }

    public static Integer getSplitNumberFiles(Configuration conf) {
        return Integer.parseInt(conf.get(SPLIT_NUMBER_FILES, "-1"));
    }


    public static void setContextSetDim(Configuration conf, long dim) {
        conf.set(CONTEXT_SET_DIM, String.valueOf(dim));
    }

    public static Long getContextSetDim(Configuration conf) {
        return Long.parseLong(conf.get(CONTEXT_SET_DIM, "1"));
    }

    public static void setMinProperty(Configuration conf, String propertyName, Double min) {
        conf.set(propertyName + MIN_PROPERTY_FIELD, min.toString());
    }

    public static Double getMinProperty(String propertyName, Configuration conf) {
        return Double.parseDouble(conf.get(propertyName + MIN_PROPERTY_FIELD));
    }

    public static void setMaxProperty(Configuration conf, String propertyName, Double max) {
        conf.set(propertyName + MAX_PROPERTY_FIELD, max.toString());
    }

    public static Double getMaxProperty(String propertyName, Configuration conf) {
        return Double.parseDouble(conf.get(propertyName + MAX_PROPERTY_FIELD));
    }

    public static String getMultiLevelMapperProperty(Configuration conf){
        return conf.get(MULTI_LEVEL_MAPPER_PROPERTY_NAME);
    }

    public static void setMultiLevelMapperProperty(String propertyName, Configuration conf){
        conf.set(MULTI_LEVEL_MAPPER_PROPERTY_NAME, propertyName);
    }

    public static void setMultiLevelParser(Class clazz, String methodName, Configuration conf) {
        conf.set(MULTI_LEVEL_PARSER_CLASS, clazz.getName());
        conf.set(MULTI_LEVEL_PARSER_METHOD_NAME, methodName);
    }

    public static String getMultiLevelParserClass(Configuration conf) {
        return conf.get(MULTI_LEVEL_PARSER_CLASS);
    }

    public static String getMultiLevelParserMethodName(Configuration conf) {
        return conf.get(MULTI_LEVEL_PARSER_METHOD_NAME);
    }

    public static Pair<Double, Double> getMinMax(String property, Configuration conf) {
        Double min = OperationConf.getMinProperty(property, conf);
        Double max = OperationConf.getMaxProperty(property, conf);
        return Pair.of(min, max);
    }

    public static void setMultiLevelOutputPath(String path, Configuration conf) {
        conf.set(MULTI_LEVEL_OUTPUT_PATH, path);
    }

    public static String getMultiLevelOutputPath(Configuration conf) {
        return conf.get(MULTI_LEVEL_OUTPUT_PATH);
    }

}