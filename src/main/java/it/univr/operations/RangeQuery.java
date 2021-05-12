package it.univr.operations;

import it.univr.descriptors.NRectangle;
import it.univr.descriptors.OneGrid;
import it.univr.hadoop.ContextData;
import it.univr.restaurant.RestaurantCSVInputFormat;
import it.univr.veronacard.VeronaCardCSVInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;

import static java.lang.String.format;
import static org.apache.hadoop.mapreduce.JobCounter.*;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

public class RangeQuery extends Configured {

  // === Attributes ============================================================

  public static final String csvMultiFormat = "CSVmulti";
  public static final String rangeQueryLabel = "rq";
  public static final String numDimsLabel = "numDims";
  static final Log LOG = LogFactory.getLog( RangeQuery.class );
  private static final int numArgs = 5;
  private static final int maxNumDims = 10;
  private static Counters counters;
  private Integer numDims;
  private Path inputPath;
  private Path outputPath;
  private String rangeQuery;
  Class<? extends FileInputFormat> inputFormatClass;


  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param args
   */

  public RangeQuery
  ( String[] args, Class<? extends FileInputFormat> inputFormatClass ) {

    if( !checkArguments( args ) ) {
      System.exit( 1 );
    }

    this.inputFormatClass = inputFormatClass;
    //System.out.printf( "Constructor!!%n" );
  }

  /**
   * MISSING_COMMENT
   *
   * @param args
   * @throws Exception
   */

  public static void main( String[] args ) throws Exception {

    final long start = System.currentTimeMillis();
    System.out.printf( "START: %d%n", start );

    /*final int res = run( new RangeQuery( args, VeronaCardCSVInputFormat.class ) );//*/
    final int res = run( new RangeQuery( args, RestaurantCSVInputFormat.class ) );//*/

    // Counters counters = firstJob.getCounters();
    // Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);

    final long end = System.currentTimeMillis();
    System.out.printf( "END: %d%n", end );
    System.out.printf( "DURATION: %d%n", ( end - start ) );

    if( counters != null ) {
      System.out.printf( "Total number of map tasks: %s.%n",
                         counters.findCounter( TOTAL_LAUNCHED_MAPS ).getValue() );
    }
    System.exit( res );
  }

  /**
   * MISSING_COMMENT
   *
   * @param q
   * @return
   * @throws Exception
   */

  public static int run( RangeQuery q ) throws Exception {
    final Configuration conf = new Configuration();
    conf.setInt( numDimsLabel, q.numDims );
    conf.set( rangeQueryLabel, q.rangeQuery );

    final Job job = Job.getInstance( conf, "rangeQuery" );
    job.setJarByClass( OneGrid.class );

    // set job input format
    job.setInputFormatClass( q.inputFormatClass );

    // set map class and the map output key and value classes
    // TO BE SPECIFIED ONLY IF THERE IS A REDUCER!!!
    // job.setMapOutputKeyClass( LongWritable.class );
    // job.setMapOutputValueClass( TextInputFormat.class );
    job.setMapperClass( RangeQueryMapper.class );

    // set reduce class and the reduce output key and value classes
    // job.setReducerClass( OneGrid.OneGridReducer.class );

    // set job output format
    job.setOutputFormatClass( TextOutputFormat.class );
    job.setOutputKeyClass( LongWritable.class );
    job.setOutputValueClass( Text.class );

    // add the input file as job input to the variable inputFile
    setInputPaths( job, q.inputPath );

    // set the output path for the job results to the variable outputPath
    setOutputPath( job, q.outputPath );

    // set the number of reducers using variable numberReducers
    // job.setNumReduceTasks( o.numReducers );

    // set the jar class
    job.setJarByClass( RangeQuery.class );

    // execute the job
    int result = job.waitForCompletion( true ) ? 0 : 1;
    counters = job.getCounters();

    return result;
  }

  /**
   * MISSING_COMMENT
   *
   * @param args
   * @return
   */

  private boolean checkArguments( String[] args ) {

    if( args.length != numArgs ) {
      printUsage();
      return false;
    }

    // 0 = CSVMulti

    try {
      numDims = Integer.parseInt( args[1] );
    } catch( NumberFormatException e ) {
      System.out.printf( "Invalid number of dimensions: %s.%n", args[0] );
      printUsage();
    }

    String rqString = args[2];
    rqString = rqString.replace( "Rectangle:", "" );
    rqString = rqString.replace( ")-(", " " );
    rqString = rqString.replace( "(", "" );
    rqString = rqString.replace( ")", "" );
    //rqString = rqString.replace( "-", "," );
    rqString = rqString.replace(',', '.');
    rqString = rqString.replace( " ", ",");
    
    rangeQuery = rqString;
    inputPath = new Path( args[3] );
    outputPath = new Path( args[4] );

    return true;
  }

  /**
   * MISSING_COMMENT
   */

  private void printUsage() {
    System.out.printf
      ( "Usage: RangeQuery "
        + "%s " // 0
        + "<dim> " // 1
        + "<rq (Rectangle: (xmin_ymin_...)_(xmax_ymax_...))> " // 2
        + "<input_path> " // 3
        + "<output_path>%n", // 4
        csvMultiFormat );
  }

  // ===========================================================================

  public static class RangeQueryMapper
    extends Mapper<LongWritable, ContextData, LongWritable, Text> {

    private int numDims;
    private NRectangle rangeQuery;

    @Override
    protected void setup( Context context ) {
      final Configuration conf = context.getConfiguration();
      numDims = conf.getInt( numDimsLabel, 2 );
      rangeQuery = new NRectangle( conf.get( rangeQueryLabel ) );
    }

    @Override
    protected void map( LongWritable key, ContextData value, Context context )
      throws IOException, InterruptedException {

      // current shape value
      final NRectangle shape = new NRectangle( value.toString().trim() );
      if( shape.isInsideMultiDims( rangeQuery ) ) {
        // check if the n-dimensional rectangle intersects the range query
        context.write( key, new Text( shape.print() ) );
      }
    }

    @Override
    protected void cleanup( Context context )
      throws IOException, InterruptedException {
      super.cleanup( context );
    }
  }
}

