package it.univr.hadoop.mapreduce.mbbox;

import it.univr.hadoop.ContextData;
import it.univr.hadoop.conf.OperationConf;
import it.univr.hadoop.conf.PartitionTechnique;
import it.univr.hadoop.util.ContextBasedUtil;
import it.univr.hadoop.util.WritablePrimitiveMapper;
import it.univr.restaurant.RestaurantCSVInputFormat;
import it.univr.util.Pair;
import it.univr.util.ReflectionUtil;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Minimum Bounding
 */
public class MBBoxMapReduce {
  static final Logger LOGGER = LogManager.getLogger( MBBoxMapReduce.class );

  public static void main( String... args ) throws IOException, InterruptedException, ClassNotFoundException {
    // todo: to remove
    // System.setProperty( "hadoop.home.dir", "/usr/local/hadoop/hadoop-3.2.1/" );

    OperationConf configuration = new OperationConf( new GenericOptionsParser( args ) );
    if( !configuration.validInputOutputFiles() ) {
      LOGGER.error( "Invalid input files" );
      System.exit( 1 );
    }
    if( !configuration.validPartitionFields() ) {
      LOGGER.error( "Invalid context fields" );
      System.exit( 1 );
    }
    Path[] inputPaths = new Path[configuration.getFileInputPaths().size()];
    configuration.getFileInputPaths().toArray( inputPaths );
    /*Pair<ContextData, ContextData> contextDataContextDataPair = runMBBoxMapReduce( configuration,
            VeronaCardCSVInputFormat.class, true );//*/
    Pair<ContextData, ContextData> contextDataContextDataPair = runMBBoxMapReduce( configuration,
            RestaurantCSVInputFormat.class, true );//*/
    Stream.of( contextDataContextDataPair.getLeft().getContextFields( configuration.partition ) ).forEach( value -> {
      Object min = ReflectionUtil.readMethod( value, contextDataContextDataPair.getLeft() );
      Object max = ReflectionUtil.readMethod( value, contextDataContextDataPair.getRight() );
      System.out.println( format( "%s ->(%s, %s)", value, min.toString(), max.toString() ) );
    } );//*/
  }

  /**
   * Retrieve Minimum bounding box of the data, by returning a context place
   * holder for minimum bound values. The return value is a context data schema
   * which is equal to the value genetic class of the FileInputFormat class
   * passed as argument.
   *
   * @param config
   * @param inputFormatClass
   * @param storeResult
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public static Pair<ContextData, ContextData> runMBBoxMapReduce
  ( OperationConf config,
    Class<? extends FileInputFormat> inputFormatClass,
    boolean storeResult )
    throws IOException, ClassNotFoundException, InterruptedException {

    Job job = Job.getInstance( config, "MBBoxMapReduce" );
    // todo: sistemare questa parte per recuperare il file di configurazione .xml!!!
    /*if( config != null && config.hContextBasedConf != null ) {
      config.hContextBasedConf.ifPresent( customConf -> {
        FileInputFormat.setMinInputSplitSize( job, customConf.getSplitSize( config.technique ) );
        FileInputFormat.setMaxInputSplitSize( job, customConf.getSplitSize( config.technique ) );
      } );
    }//*/

    final long splitSize;
    if( config.getHContextBasedConf() != null ){
      splitSize = config.getHContextBasedConf().get().getSplitSize( PartitionTechnique.BOX_COUNT );
    } else {
      splitSize = 1024*32;
    }

    FileInputFormat.setMinInputSplitSize( job, splitSize );
    FileInputFormat.setMaxInputSplitSize( job, splitSize );

    // if the Map and Reduce classes are not inner classes, the command
    // setJarByClass specifies that the Mapper and Reducer implementations
    // have to be present as part of that jar
    job.setJarByClass( MBBoxMapReduce.class );

    // if( storeResult ) {
    //  job.setJarByClass( MBBoxMapReduce.class );
    // }
    Path[] inputPaths = new Path[config.getFileInputPaths().size()];
    config.getFileInputPaths().toArray( inputPaths );

    job.setMapperClass( MBBoxMapper.class );
    job.setMapOutputKeyClass( Text.class );
    job.setMapOutputValueClass( ObjectWritable.class );
    job.setCombinerClass( MBBoxCombiner.class );
    job.setReducerClass( MBBoxReducer.class );


    Optional<? extends ContextData> minContextData = ContextBasedUtil
      .getContextDataInstanceFromInputFormat( inputFormatClass );
    Optional<? extends ContextData> maxContextData = ContextBasedUtil
      .getContextDataInstanceFromInputFormat( inputFormatClass );
    if( maxContextData.isPresent() && minContextData.isPresent() ) {
      //input setup
      job.setInputFormatClass( inputFormatClass );
      FileInputFormat.setInputPaths( job, inputPaths );

      //output
      FileOutputFormat.setOutputPath( job, config.getOutputPath() );
      job.setOutputFormatClass( TextOutputFormat.class );
      job.waitForCompletion( true );

      Counters counters = job.getCounters();
      Counter outputRecordCounter = counters.findCounter( JobCounter.TOTAL_LAUNCHED_REDUCES );
      outputRecordCounter.getValue();
      ContextData mbrMaxContextData = maxContextData.get();
      ContextData mbrMinContextData = minContextData.get();
      FileSystem fileSystem = FileSystem.get( config );
      for( FileStatus fileStatus : fileSystem.listStatus( config.getOutputPath() ) ) {
        if( !fileStatus.isDirectory() ) {
          KeyValueLineRecordReader reader =
            new KeyValueLineRecordReader
              ( config,
                new FileSplit( fileStatus.getPath(),
                               0,
                               fileStatus.getLen(),
                               new String[0] ) );
          Text key = reader.createKey();
          Text value = reader.createValue();
          boolean min = true;
          while( reader.next( key, value ) ) {
            try {
              ContextData contextData;
              if( min ) {
                contextData = mbrMinContextData;
                min = false;
              } else {
                contextData = mbrMaxContextData;
                min = true;
              }
              PropertyDescriptor descriptor = new PropertyDescriptor( key.toString(),
                                                                      contextData.getClass() );
              Object propertyValue = WritablePrimitiveMapper.getBeanObjectFromText( value, descriptor.getPropertyType() );
              descriptor.getWriteMethod().invoke( contextData, propertyValue );
              LOGGER.info( format( "MBR output field: %s, value %s", key, value ) );
            } catch( IllegalAccessException | InvocationTargetException | IntrospectionException e ) {
              e.printStackTrace();
            }
          }
          reader.close();
        }
      }

      if( !storeResult )
        fileSystem.delete( config.getOutputPath(), true );
      Pair<ContextData, ContextData> maxMinContextData = Pair.of( minContextData.get(), maxContextData.get() );
      return maxMinContextData;
    }
    return null;
  }
}
