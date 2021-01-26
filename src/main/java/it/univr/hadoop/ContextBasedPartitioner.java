package it.univr.hadoop;

import it.univr.descriptors.OneGrid;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.conf.PartitionTechnique;
import it.univr.hadoop.mapreduce.boxcount.BoxCountingReducer;
import it.univr.hadoop.mapreduce.multidim.MultiDimMapper;
import it.univr.hadoop.mapreduce.multidim.MultiDimReducer;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridMapper;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelGridReducer;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelMiddleMapper;
import it.univr.hadoop.output.ContextBasedTextOutputFormat;
import it.univr.hadoop.mapreduce.boxcount.BoxCountingMapper;
import it.univr.hadoop.mapreduce.multilevel.MultiLevelMiddleReducer;
import it.univr.util.Pair;
import it.univr.util.ReflectionUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static it.univr.hadoop.mapreduce.mbbox.MBBoxMapReduce.runMBBoxMapReduce;
import static it.univr.hadoop.util.ContextBasedUtil.*;
import static java.lang.Math.ceil;
import static java.lang.String.format;

/**
 * Main class to run a context based partition
 *
 * @author Gurjeet Singh
 * @version 0.8 partially rev. Sara Migliorini
 */

public class ContextBasedPartitioner {
  static final Logger LOGGER = LogManager.getLogger( ContextBasedPartitioner.class );

  String[] args;
  Class<? extends FileInputFormat> inputFormatClass;

  /**
   * MISSING_COMMENT
   *
   * @param args
   * @param inputFormatClass
   */

  public ContextBasedPartitioner
  ( String[] args, Class<? extends FileInputFormat> inputFormatClass ) {
    this.args = args;
    this.inputFormatClass = inputFormatClass;
  }

  /**
   * MISSING_COMMENT
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */

  public void runPartitioner()
    throws Exception {

    // load the configuration for the job in particular the file conf.xml
    // containing the split size
    final OperationConf config = new OperationConf( new GenericOptionsParser( args ) );

    // retrieve input format output class
    final Class<? extends ContextData> concreteDataClass =
      getContextDataClassFromInputFormat( inputFormatClass ).get();

    OperationConf.setMultiLevelParser
      ( concreteDataClass, ContextData.PARSE_RECORD_METHOD, config );
    if( !config.validInputOutputFiles() ) {
      LOGGER.error( "Invalid input files" );
      System.exit( 1 );
    }
    if( !config.validPartitionFields() ) {
      LOGGER.error( "Invalid context fields" );
      System.exit( 1 );
    }
    // Retrieve Minimum Bounding Box Map-Reduce result
    final Pair<ContextData, ContextData> mmbox = runMBBoxMapReduce
      ( config, inputFormatClass, false );

    final int numDims = mmbox.getLeft().getContextFields( config.partition ).length;

    if( mmbox != null ) {
      Job job = getJob( mmbox.getLeft(), mmbox.getRight(), config );
      // Mapper setup
      if( config.technique == PartitionTechnique.MD_GRID ) {
        configureMultiDimPartitioner( job );
      } else if( config.technique == PartitionTechnique.ML_GRID ) {
        runMultiLevelPartitioner( job, config, mmbox.getLeft() );
        return;
      } else if( config.technique == PartitionTechnique.BOX_COUNT ) {
        configureBoxCountPartitioner
          ( job,
            config.getFileInputPaths().get( 0 ).toUri().getRawPath(),
            config.getGridPath().toUri().getRawPath(),
            numDims,
            config.contextFields,
            config.cellSide );
      }

      // output
      LazyOutputFormat.setOutputFormatClass( job, ContextBasedTextOutputFormat.class );
      FileOutputFormat.setOutputPath( job, config.getOutputPath() );

      // job
      job.waitForCompletion( true );
      return;
    }
    LOGGER.warn( "No data found" );
  }

  /**
   * The method configures and gets the Job made by commons information for
   * every partition technique
   *
   * @param minContextDataValue
   * @param maxContextDataValue
   * @param config
   * @return
   * @throws IOException
   */

  private Job getJob
  ( ContextData minContextDataValue,
    ContextData maxContextDataValue,
    OperationConf config ) throws IOException {

    // TODO Passing a json string as parameter configuration could be another way to access to needed information from Reducer and Mappers.
    final String[] fields = minContextDataValue.getContextFields( config.partition );
    // input setup
    OperationConf.setContextSetDim( config, fields.length );
    OperationConf.setPartitionFields( config, config.partition );
    // width calculation
    Stream.of( fields ).forEach( property -> {
      Comparable<?> maxValue = (Comparable<?>) ReflectionUtil.readMethod( property, maxContextDataValue );
      Comparable<?> minValue = (Comparable<?>) ReflectionUtil.readMethod( property, minContextDataValue );
      final Double max = Double.valueOf( maxValue.toString() );
      final Double min = Double.valueOf( minValue.toString() );
      OperationConf.setMinProperty( config, property, min );
      OperationConf.setMaxProperty( config, property, max );

    } );

    if( config.technique.equals( PartitionTechnique.ML_GRID ) ) {
      OperationConf.setMultiLevelMapperProperty( fields[0], config );
      OperationConf.setMultiLevelOutputPath( config.getOutputPath().toUri().getPath(), config );
    }
    OperationConf.setMasterFileEnabled( config, true );

    final Job job = Job.getInstance( config, "CBMR" );
    // if the Map and Reduce classes are not inner classes, the command
    // setJarByClass specifies that the Mapper and Reducer implementations
    // have to be present as part of that jar
    job.setJarByClass( ContextBasedPartitioner.class );

    final Path[] inputPaths = new Path[config.getFileInputPaths().size()];
    config.getFileInputPaths().toArray( inputPaths );
    configureSplitSize( config, job );
    FileInputFormat.setInputPaths( job, inputPaths );
    job.setInputFormatClass( inputFormatClass );
    return job;
  }

  /**
   * MISSING_COMMENT
   *
   * @param job
   */

  private void configureMultiDimPartitioner( Job job ) {
    Class<?> mapOutputValueClass = ContextData.class;
    Optional<Class<? extends ContextData>> present =
      getContextDataClassFromInputFormat( inputFormatClass );
    if( present.isPresent() ) {
      mapOutputValueClass = present.get();
    }
    job.setMapOutputKeyClass( Text.class );
    job.setMapOutputValueClass( mapOutputValueClass );
    job.setMapperClass( MultiDimMapper.class );
    //Reducer
    job.setReducerClass( MultiDimReducer.class );
  }

  /**
   * Run multi level partition technique by creating temporary files and
   * folders, for each attribute partition.
   *
   * @param job
   * @param config
   * @param contextData
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */

  private void runMultiLevelPartitioner( Job job, OperationConf config, ContextData contextData )
    throws IOException, ClassNotFoundException, InterruptedException {
    Class<?> mapOutputValueClass = ContextData.class;
    Optional<Class<? extends ContextData>> present =
      getContextDataClassFromInputFormat( inputFormatClass );
    if( present.isPresent() ) {
      mapOutputValueClass = present.get();
    }
    int i = 0;
    Path directoryOutputPath = config.getOutputPath();

    for( String propertyName : contextData.getContextFields( config.partition ) ) {
      job.setMapOutputKeyClass( Text.class );
      job.setMapOutputValueClass( mapOutputValueClass );
      if( i == 0 )
        job.setMapperClass( MultiLevelGridMapper.class );
      else
        job.setMapperClass( MultiLevelMiddleMapper.class );
      if( i == contextData.getContextFields( config.partition ).length - 1 ) {
        job.setReducerClass( MultiLevelGridReducer.class );
        LazyOutputFormat.setOutputFormatClass( job, ContextBasedTextOutputFormat.class );
      } else {
        job.setReducerClass( MultiLevelMiddleReducer.class );
        LazyOutputFormat.setOutputFormatClass( job, TextOutputFormat.class );
      }
      Path outputPath = new Path( config.getOutputPath(), propertyName );
      FileOutputFormat.setOutputPath( job, outputPath );
      LOGGER.warn( outputPath.toUri().getPath() );
      boolean completed = job.waitForCompletion( true );
      if( !completed )
        break;
      i++;
      //Prepare new job for the next partition level
      if( i < contextData.getContextFields( config.partition ).length ) {
        OperationConf.setMultiLevelMapperProperty( contextData.getContextFields( config.partition )[i], config );
        FileSystem fileSystem = FileSystem.get( config );
        LOGGER.warn( "Next property is: " + OperationConf.getMultiLevelMapperProperty( config ) );
        job = Job.getInstance( config, "CBMR" );
        job.setJarByClass( ContextBasedPartitioner.class );
        configureSplitSize( config, job );
        Vector<Path> newInputs = new Vector<>( Arrays.stream( fileSystem.listStatus( outputPath ) )
                                                     .filter( f -> f.isFile() )
                                                     .map( f -> f.getPath() )
                                                     .filter( p -> p.getName().contains( MultiLevelGridMapper.START_CAPTION_FILE ) )
                                                     .collect( Collectors.toList() ) );
        config.setFileInputPaths( newInputs );
        config.setOutputDirectory( directoryOutputPath );
        LOGGER.warn( "LAST OUT OUTPATH " + config.getOutputPath().toUri().getPath() );
        Path[] filePaths = new Path[newInputs.size()];
        newInputs.toArray( filePaths );
        LOGGER.warn( Arrays.stream( filePaths ).map( Path::toString ).collect( Collectors.joining( ", " ) ) );
        FileInputFormat.setInputPaths( job, filePaths );
        job.setInputFormatClass( KeyValueTextInputFormat.class );
      } else {
        //Move result to the right output folder and delete all tmp folders
        FileSystem fileSystem = FileSystem.get( config );

        List<Path> tmpFolders = Stream.of( fileSystem.listStatus( config.getOutputPath() ) )
                                      .filter( FileStatus::isDirectory )
                                      .map( FileStatus::getPath )
                                      .filter( p -> !p.getName().contains( propertyName ) )
                                      .collect( Collectors.toList() );
        for( Path folder : tmpFolders ) {
          fileSystem.delete( folder, true );
        }

        FileStatus directoryData = Stream.of( fileSystem.listStatus( config.getOutputPath() ) )
                                         .filter( FileStatus::isDirectory )
                                         .findFirst().get();

        Path[] filesToMove = Stream.of( fileSystem.listStatus( directoryData.getPath() ) ).filter( FileStatus::isFile )
                                   .map( FileStatus::getPath ).toArray( Path[]::new );
        fileSystem.moveFromLocalFile( filesToMove, config.getOutputPath() );
        fileSystem.delete( directoryData.getPath(), false );
      }
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param job
   * @param inputPath
   * @param gridPath
   * @param numDims
   * @throws Exception
   */

  private void configureBoxCountPartitioner
  ( Job job,
    String inputPath,
    String gridPath,
    Integer numDims,
    String contextFields,
    Double cellSide )
    throws Exception {

    final String[] args = new String[]{
      OneGrid.csvMultiFormat,
      numDims.toString(),
      OneGrid.computeMbrPar,
      cellSide.toString(),
      "noMI",
      contextFields,
      inputPath,
      gridPath,
    };

    final OneGrid oneGrid = new OneGrid( args, inputFormatClass );
    final int res = OneGrid.run( oneGrid );

    final FileSystem fileSystem = FileSystem.get( job.getConfiguration() );
    for( FileStatus fileStatus : fileSystem.listStatus( new Path( gridPath ) ) ) {
      if( !fileStatus.isDirectory() && fileStatus.getPath().getName().contains( "part-" ) ) {
        job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
        final KeyValueLineRecordReader reader =
          new KeyValueLineRecordReader
            ( job.getConfiguration(),
              new FileSplit( fileStatus.getPath(),
                             0,
                             fileStatus.getLen(),
                             new String[0] ) );
        final Text key = reader.createKey();
        final Text value = reader.createValue();
        Integer i = 0;
        while( reader.next( key, value ) ) {
          if( key.toString().startsWith( format( "Partition[%d]", i ) )) {
            job.getConfiguration().set( format( "part-%s", i ), value.toString() );
            i = i+1;
          }
        }
        job.getConfiguration().set( "dimensions", i.toString() );
        reader.close();
      }
    }

    Class<?> mapOutputValueClass = ContextData.class;
    Optional<Class<? extends ContextData>> present =
      getContextDataClassFromInputFormat( inputFormatClass );
    if( present.isPresent() ) {
      mapOutputValueClass = present.get();
    }
    job.setMapOutputKeyClass( Text.class );
    job.setMapOutputValueClass( mapOutputValueClass );
    job.setMapperClass( BoxCountingMapper.class );
    job.setReducerClass( BoxCountingReducer.class );//*/
  }

  /**
   * Configure split size by loading conf configuration file. Furthermore
   * calculate number of splits needed.
   *
   * @param config
   * @param job
   * @throws IOException
   */

  private void configureSplitSize( OperationConf config, Job job ) throws IOException {
    if( config.hContextBasedConf.isPresent() ) {
      // todo check how to load conf.xml
      // final Long splitSize = config.hContextBasedConf.get().getSplitSize( config.technique );
      final Long splitSize = new Long( 1024*1024*128 );
      FileInputFormat.setMinInputSplitSize( job, splitSize );
      FileInputFormat.setMaxInputSplitSize( job, splitSize );

      int splitNumberFiles = OperationConf.getSplitNumberFiles( job.getConfiguration() );
      if( splitNumberFiles < 0 ) {
        //If split number not yet set
        FileSystem fileSystem = FileSystem.get( config );
        long totalSize = 0;
        for( Path fileInputPath : config.getFileInputPaths() ) {
          totalSize += fileSystem.getFileStatus( fileInputPath ).getLen();
          LOGGER.warn( "File size is " + fileSystem.getFileStatus( fileInputPath ).getLen() );
        }
        // Plus 1 cause of coherence with input format class, which return number of splits
        splitNumberFiles = (int) ceil( totalSize / splitSize ) + 1;
        OperationConf.setSplitNumberFiles( config, splitNumberFiles );
        LOGGER.info( format( "Splits number are: %d", splitNumberFiles ) );
      }
    } else {
      //TODO Possible alternative for default split size from Hadoop system
    }
  }

}
