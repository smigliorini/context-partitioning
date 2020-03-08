package it.univr.veronacard.partitioning;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static it.univr.veronacard.partitioning.DataUtils.*;
import static it.univr.veronacard.partitioning.FileUtils.printSize;
import static it.univr.veronacard.partitioning.FileUtils.readLines;
import static it.univr.veronacard.partitioning.PartUtils.*;
import static it.univr.veronacard.partitioning.QueryUtils.rangeContextQuery;
import static it.univr.veronacard.partitioning.StatsUtils.buildStatFile;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class MultiDimPartitioning {

  //private static final String dataDir = "C:\\workspace\\projects\\veronacard_analysis\\test\\test_partitioning\\";
  private static final String dataDir = "/home/sangra/Documenti/UniVr/Tirocinio/veronacard/test/test_partitioning/";
  //private static final String dataDir = "/workspace/projects/veronacard/test/test_partitioning/";
  private static final String dataFile = "vc_ticket_space_time.csv";
  private static final String outFile = "vc_ticket_space_time_transf.csv";
  private static final String fractalFile = "vc_ticket_space_time_fractal.csv";

  private static final String randomPartDir = "part_random";
  private static final String mdGridPartDir = "md_part_grid";
  private static final String mlGridPartDir = "ml_part_grid";
  private static final String cbPartDir = "cb_part_grid";
  private static final String partPrefix = "part-";

  private static final String[] attributes = {
    "vcSerial",
    "geometry",
    "timestamp",
    "poiName",
    "age"
  };

  private static final String separator = ",";


  // split dimension in bytes = 1Mbyte
  private static final int splitSize = 1024 * 1024;


  // === Methods ===============================================================

  public static void main( String[] args )
    throws IOException, ParseException {

    // --- transform inputs ----------------------------------------------------

    //transformInput();
    //transformFractalInput();

    // --- build indexes -------------------------------------------------------

    buildIndexes();

    // --- compute stats -------------------------------------------------------

    System.out.printf( "Compute statistics for Random partitioning%n" );
    buildStatFile
            ( new File( dataDir, randomPartDir ),
                    new File( dataDir, "stats_random.csv"),
                    4, separator, partPrefix, false );//*/

    final boolean countSplitsSeparately = true;

    System.out.printf( "Compute statistics for MultiDimensionalUniformGrid partitioning%n" );
    buildStatFile
      ( new File( dataDir, mdGridPartDir ),
        new File( dataDir, countSplitsSeparately ? "stats_multi_dim_uniform_grid_splits.csv" :  "stats_multi_dim_uniform_grid.csv" ),
        4, separator, partPrefix, countSplitsSeparately );

    System.out.printf( "Compute statistics for MultiLevelUniformGrid partitioning%n" );//*/
    buildStatFile
      ( new File( dataDir, mlGridPartDir ),
        new File( dataDir, countSplitsSeparately ? "stats_multi_level_uniform_grid_splits.csv" : "stats_multi_level_uniform_grid.csv" ),
        4, separator, partPrefix, countSplitsSeparately );//*/

    System.out.printf( "Compute statistics for ContextBased partitioning%n" );//*/
    buildStatFile
      ( new File( dataDir, cbPartDir ),
        new File( dataDir, "stats_context_based_splits.csv" ),
        4, separator, partPrefix, countSplitsSeparately );//*/

    // --- range queries -------------------------------------------------------

    testQueries();
    //countAges( "vc_tickets_by_ages.csv" );
  }

  // ===========================================================================

  private static void transformFractalInput() throws FileNotFoundException {
    final File input = new File( dataDir, outFile );
    final long size = input.length();
    printSize( size );

    final List<String> lines = readLines( input, false );
    System.out.printf( "Number of read lines: %d.%n", lines.size() );
    computeBoundaries( lines, separator );

    buildFractalInput( lines, dataDir, fractalFile, separator );
  }


  private static void transformInput() throws FileNotFoundException {
    final File input = new File( dataDir, dataFile );
    final long size = input.length();
    printSize( size );

    final List<String> lines = readLines( input, false );
    System.out.printf( "Number of read lines: %d.%n", lines.size() );

    transformLines( lines, dataDir, outFile, separator );
  }


  private static void countAges( String outputFile ) throws IOException {

    final File input = new File( dataDir, outFile );
    final List<String> lines = readLines( input, false );
    final List<Record> records = parseRecords( lines, separator );

    final Map<Integer,Integer> result = new HashMap<>();
    for( Record r : records ){
      final int k = r.getAge() / 10;
      Integer v = result.get( k );
      if( v == null ){
        v = 0;
      }
      v = v+1;
      result.put( k, v );
    }

    try( BufferedWriter bw = new BufferedWriter( new FileWriter( outputFile ) ) ) {
      bw.write(String.format("Age%s"
                      + "NumRows%n",
              separator));

      final List<Integer> keys = new ArrayList<Integer>(result.keySet());
      Collections.sort(keys);

      for (Integer k : keys) {
        bw.write(String.format("%s%s"
                        + "%s"
                        + "%n",
                k * 10, separator,
                result.get(k), separator));
      }
    }
  }

  private static void buildIndexes() throws IOException {
    final File input = new File( dataDir, outFile );
    final long size = input.length();
    printSize( size );

    final List<String> lines = readLines( input, false );
    System.out.printf( "Number of read lines: %d.%n", lines.size() );

    final Boundaries b = computeBoundaries( lines, separator );
    System.out.printf( "X boundaries (%.4f,%.4f).%n", b.getMinX(), b.getMaxX() );
    System.out.printf( "Y boundaries (%.4f,%.4f).%n", b.getMinY(), b.getMaxY() );
    System.out.printf( "Time boundaries (%d,%d).%n", b.getMinT(), b.getMaxT() );
    System.out.printf( "Age boundaries (%d,%d).%n", b.getMinAge(), b.getMaxAge() );


    //generateRandomParts( input, new File( dataDir, randomPartDir ), splitSize, partPrefix );
    generateUniformMultiDimGridParts( input, new File( dataDir, mdGridPartDir ), splitSize, partPrefix, b, separator );
    //generateUniformMultiLevelGridParts( input, new File( dataDir, mlGridPartDir ), splitSize, partPrefix, b, separator );

    final double[] timeSplits = new double[] {
            1388575020000.000000,
            1422394200000.000000,
            1456213380000.000000,
            1490032560000.000000 };

    final double[] xSplits = new double[]{
            10.979600,
            10.993500,
            10.996975,
            10.998713,
            11.000450,
            11.007400
    };

    final double[] ySplits = new double[]{
            45.433300,
            45.436800,
            45.440300,
            45,442050,
            45,442925,
            45,443800,
            45,447300
    };

    final double[] aSplits = new double[]{
            10.000000,
            30.000000,
            46.015625,
            90.000000
    };

    generateContextBasedParts( input, new File( dataDir, cbPartDir ), separator, partPrefix, splitSize, timeSplits, xSplits, ySplits, aSplits );
  }


  private static void testQueries() throws ParseException {
    final SimpleDateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:ss" );

    // castel vecchio
    // 10.9878,45.4397

      // Range query t=[2015-03-01 08:00,2015-05-31 18:00],x=[10.98930,10.99490], y=[45.43042,45.44050], age=[20,30]
      // Range query t=[null,null],x=[null,null], y=[null,null], age=[30,60]
      // Range query t=[2016-01-01 07:00,2016-12-31 23:00],x=[null,null], y=[null,null], age=[10,20]
      // Range query t=[null,null],x=[10.99000,11.00000], y=[45.43000,45.44000], age=[75,90]

    final double minX = 10.99000;
    final double maxX = 11.00000;
    //final Double minX = null;
    //final Double maxX = null;

    final double minY = 45.43000;
    final double maxY = 45.44000;
    //final Double minY = null;
    //final Double maxY = null;

    //final String minT = "2016-01-01 07:00";
    //final String maxT = "2016-12-31 23:00";
    final String minT = null;
    final String maxT = null;

    final int minAge = 85;
    final int maxAge = 90;
    //final Integer minAge = null;
    //final Integer maxAge = null;

    final QueryParams params = new QueryParams
      ( minX, maxX,
        minY, maxY,
        minT != null ? f.parse( minT ).getTime() : null,
        maxT != null ? f.parse( maxT ).getTime() : null,
        minAge, maxAge );

    final Set<String> numSplitsR =
      rangeContextQuery( params, new File( dataDir, randomPartDir ), separator, partPrefix );

    final Set<String> numSplitsMdg =
      rangeContextQuery( params, new File( dataDir, mdGridPartDir ), separator, partPrefix );

    final Set<String> numSplitsMlg =
      rangeContextQuery( params, new File( dataDir, mlGridPartDir ), separator, partPrefix );

    final Set<String> numSplitsCbp =
            rangeContextQuery( params, new File( dataDir, cbPartDir ), separator, partPrefix );


    final String minTString = minT != null ? f.format( new Date( params.getMinT() ) ) : "null";
    final String maxTString = maxT != null ? f.format( new Date( params.getMaxT() ) ) : "null";

    System.out.printf
      ( "Range query "
        + "t=[%s,%s],"
        + "x=[%.5f,%.5f], "
        + "y=[%.5f,%.5f], "
        + "age=[%d,%d]%n",
        minTString,
        maxTString,
        params.getMinX(), params.getMaxX(),
        params.getMinY(), params.getMaxY(),
        params.getMinAge(), params.getMaxAge() );

    System.out.printf( "Num splits with RAND partitioning: %d.%n", numSplitsR.size() );
    System.out.printf( "Num splits with multi-dimensional uniform grid: %d.%n", numSplitsMdg.size() );

    // num levels = num dimensions + 1
    final Integer[] counters = splitsPerLevel( numSplitsMlg, partPrefix, 5 );
    System.out.printf( "Num splits with multi-level uniform grid: %d => ", numSplitsMlg.size() );
    for( int i = 0; i < counters.length; i++ ) {
      System.out.printf( "l%d:%d", i, counters[i] );
      if( i < counters.length - 1 ) {
        System.out.printf( ", " );
      }
    }
    System.out.printf( "%n" );

      System.out.printf( "Num splits with CBP partitioning: %d.%n", numSplitsCbp.size() );
  }
}
