package it.univr.restaurant.partitioning;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import static it.univr.restaurant.partitioning.DataUtils.*;
import static it.univr.partitioning.FileUtils.printSize;
import static it.univr.partitioning.FileUtils.readLines;
import static it.univr.restaurant.partitioning.PartUtils.*;
//import static it.univr.partitioning.QueryUtils.rangeContextQuery;
import static it.univr.restaurant.partitioning.StatsUtils.buildStatFile;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class MultiDimPartitioning {

  private static final String dataDir = "/home/hduser/Gitrepohd/context-partitioning/test/test_partitioning/";
  private static final String dataFile = "restaurantsNumber.csv";
  private static final String outFile = "restaurantsNumber.csv";

  private static final String fractalFile = "restaurantsPart_fractal.csv";

  private static final String randomPartDir = "part_random";
  private static final String mdGridPartDir = "md_part_grid";
  private static final String mlGridPartDir = "ml_part_grid";
  private static final String cbPartDir = "cb_part_grid";
  private static final String partPrefix = "part-";

  private static final String[] attributes = {
          "building",
          "coordX",
          "coordY",
          "borough",
          "cuisine",
          "$date",
          "grade",
          "score",
          "name",
          "restaurantId"
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
        4, separator, partPrefix, countSplitsSeparately );//*/

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

    //testQueries();
    //
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

  private static void buildIndexes() throws IOException {
    final File input = new File( dataDir, outFile );
    final long size = input.length();
    printSize( size );

    final List<String> lines = readLines( input, false );
    System.out.printf( "Number of read lines: %d.%n", lines.size() );

    final Boundaries b = computeBoundaries( lines, separator );
    System.out.printf( "CoordX boundaries (%.4f,%.4f).%n", b.getMinX(), b.getMaxX() );
    System.out.printf( "CoordY boundaries (%.4f,%.4f).%n", b.getMinY(), b.getMaxY() );
    System.out.printf( "Date boundaries (%d,%d).%n", b.getMinT(), b.getMaxT() );
    System.out.printf( "Score boundaries (%d,%d).%n", b.getMinScore(), b.getMaxScore() );


    //generateRandomParts( input, new File( dataDir, randomPartDir ), splitSize, partPrefix );
    generateUniformMultiDimGridParts( input, new File( dataDir, mdGridPartDir ), splitSize, partPrefix, b, separator );
    generateUniformMultiLevelGridParts( input, new File( dataDir, mlGridPartDir ), splitSize, partPrefix, b, separator );

    // TODO
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
    //generateContextBasedParts( input, new File( dataDir, cbPartDir ), separator, partPrefix, splitSize, timeSplits, xSplits, ySplits, aSplits );
  }

}
