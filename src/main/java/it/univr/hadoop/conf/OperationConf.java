package it.univr.hadoop.conf;

import it.univr.hadoop.ContextData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
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

    public Optional<HContexBasedConf> hContextBasedConf;
    public Vector<Path> filePaths;
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

        filePaths = new Vector(Stream.of(args)
                .filter(s -> s.contains("/") || s.contains("\\"))
                .map(s -> new Path(s))
                .collect(Collectors.toList()));

        technique = Stream.of(args).filter(arg -> arg.contains(PartitionTechnique.BOUX_COUNT.getPartitionTechnique())
                || arg.contains(PartitionTechnique.MD_GRID.getPartitionTechnique())
                || arg.contains(PartitionTechnique.ML_GRID.getPartitionTechnique())).map(s -> PartitionTechnique.valueOf(s))
                .findFirst().orElse(PartitionTechnique.MD_GRID);

    }

    /**
     * @return Return Validation of files. In case of the no output directory a default will be added in the same
     * directory of the first input file
     */
    public boolean validInputOutputFiles() {
        if(filePaths.size() == 0)
            return false;
        if(filePaths.size() == 1) {
            Path path = filePaths.get(0);
            try {
                FileSystem fs = path.getFileSystem(this);
                if(fs.exists(path)) {

                    Path directory = new Path(path.getParent().toString()+File.separator +"out");
                    LOGGER.warn(format("The output directory is not present, a default one has been added in to %s",
                            directory.getName()));
                    filePaths.add(filePaths.size(), directory);
                } else {
                    return false;
                }
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        for(Path path : filePaths.subList(0, filePaths.size()-1)) {
            try {
                if(!path.getFileSystem(this).exists(path))
                    return false;
            } catch (IOException e) {
                LOGGER.error(e.getLocalizedMessage());
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public Optional<HContexBasedConf> getHContextBasedConf() {
        return hContextBasedConf;
    }

    public Vector<Path> getFileInputPaths() {
        return new Vector<>(filePaths.subList(0, filePaths.size()-1));
    }

    public Path getOutputPath() {
        return filePaths.get(filePaths.size()-1);
    }

    public PartitionTechnique getTechnique() {
        return technique;
    }

    public static void setSplitNumberFiles(Configuration conf, long splitNumberFiles) {
        conf.set(SPLIT_NUMBER_FILES, String.valueOf(splitNumberFiles));
    }

    public static Long getSplitNumberFiles(Configuration conf) {
        return Long.parseLong(conf.get(SPLIT_NUMBER_FILES));
    }


    public static void setContextSetDim(Configuration conf, long dim) {
        conf.set(CONTEXT_SET_DIM, String.valueOf(dim));
    }

    public static Long getContextSetDim(Configuration conf) {
        return Long.parseLong(conf.get(CONTEXT_SET_DIM));
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


}
