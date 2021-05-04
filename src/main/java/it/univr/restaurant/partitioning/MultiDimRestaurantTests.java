package it.univr.restaurant.partitioning;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static it.univr.partitioning.FileUtils.printSize;
import static it.univr.partitioning.FileUtils.readLines;
import static it.univr.partitioning.PartUtils.*;

import static it.univr.restaurant.partitioning.DataUtils.*;
//import static it.univr.restaurant.partitioning.PartRestaurant.generateContextBasedParts;
import static it.univr.restaurant.partitioning.QueryRestaurant.rangeContextQuery;
import static it.univr.restaurant.partitioning.StatsRestaurant.buildStatFile;


/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class MultiDimRestaurantTests {

  private static final String dataDir = "/home/hduser/Gitrepohd/context-partitioning/test/test_partitioning/";
  private static final String dataFile = "restaurants2.csv";
  private static final String outFile = "restaurants2.csv";

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
          "time",
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
            5, separator, partPrefix, false );//*/
  
    final boolean countSplitsSeparately = true;
  
    System.out.printf( "Compute statistics for MultiDimensionalUniformGrid partitioning%n" );
    buildStatFile
        ( new File( dataDir, mdGridPartDir ),
            new File( dataDir, countSplitsSeparately ? "stats_multi_dim_uniform_grid_splits.csv" :  "stats_multi_dim_uniform_grid.csv" ),
            5, separator, partPrefix, countSplitsSeparately );//*/
  
    System.out.printf( "Compute statistics for MultiLevelUniformGrid partitioning%n" );//*/
    buildStatFile
        ( new File( dataDir, mlGridPartDir ),
            new File( dataDir, countSplitsSeparately ? "stats_multi_level_uniform_grid_splits.csv" : "stats_multi_level_uniform_grid.csv" ),
            5, separator, partPrefix, countSplitsSeparately );//*/
  
    System.out.printf( "Compute statistics for ContextBased partitioning%n" );//*/
    buildStatFile
        ( new File( dataDir, cbPartDir ),
            new File( dataDir, "stats_context_based_splits.csv" ),
            5, separator, partPrefix, countSplitsSeparately );//*/
  
    // --- range queries -------------------------------------------------------
  
    //testQueries();
  }
  
  // ===========================================================================
  
  private static void transformFractalInput() throws IOException {
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
  
    //final Boundaries b = computeBoundaries( lines, separator );
    final RestaurantBoundaries b = computeBoundaries( lines, separator );
    /*
    System.out.printf( "CoordX boundaries (%.4f,%.4f).%n", b.getMinX(), b.getMaxX() );
    System.out.printf( "CoordY boundaries (%.4f,%.4f).%n", b.getMinY(), b.getMaxY() );
    System.out.printf( "Zipcode boundaries (%d,%d).%n", b.getMinZipcode(), b.getMaxZipcode() );
    System.out.printf( "Date boundaries (%d,%d).%n", b.getMinT(), b.getMaxT() );
    System.out.printf( "Score boundaries (%d,%d).%n", b.getMinScore(), b.getMaxScore() );
    System.out.printf( "Id boundaries (%d,%d).%n", b.getMinId(), b.getMaxId() );//*/
    System.out.printf( b.toString() );
    
    //generateRandomParts( input, new File( dataDir, randomPartDir ), splitSize, partPrefix );//*/
    generateUniformMultiDimGridParts( input, new File( dataDir, mdGridPartDir ), splitSize, partPrefix, b, separator );//*/
    generateUniformMultiLevelGridParts( input, new File( dataDir, mlGridPartDir ), splitSize, partPrefix, b, separator );//*/
    generateContextBasedParts( input, new File( dataDir, cbPartDir ), separator, partPrefix, splitSize, b );//*/
    // todo: remove duplicate
    /*final double[] timeSplits = new double[] {
    };
    
    final double[] xSplits = new double[] {
    };
    
    final double[] ySplits = new double[] {
    };
    
    final double[] zSplits = new double[] {
    };
    
    final double[] sSplits = new double[] {
    };
    
    final double[] idSplits = new double[] {
    };
    
    generateContextBasedParts( input, new File( dataDir, cbPartDir ), separator, partPrefix, splitSize,
        timeSplits, xSplits, ySplits, zSplits, sSplits, idSplits );//*/
  }
  
  private static void testQueries() throws ParseException {
    final SimpleDateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:ss" );
    
    
    final Double minX = null;
    final Double maxX = null;
    
    final Double minY = null;
    final Double maxY = null;
  
    final Integer minZ = null;
    final Integer maxZ = null;

    final String minT = null;
    final String maxT = null;
    
    final Integer minS = null;
    final Integer maxS = null;
  
    final Integer minId = null;
    final Integer maxId = null;
    
    
    final QueryRestaurant params = new QueryRestaurant( new QueryParamsRestaurant
        ( minX, maxX,
            minY, maxY,
            minZ, maxZ,
            minT != null ? f.parse( minT ).getTime() : null,
            maxT != null ? f.parse( maxT ).getTime() : null,
            minS, maxS,
            minId, maxId ) );
    
    final Set<String> numSplitsR =
        rangeContextQuery( params, new File( dataDir, randomPartDir ), separator, partPrefix );
    
    final Set<String> numSplitsMdg =
        rangeContextQuery( params, new File( dataDir, mdGridPartDir ), separator, partPrefix );
    
    final Set<String> numSplitsMlg =
        rangeContextQuery( params, new File( dataDir, mlGridPartDir ), separator, partPrefix );
    
    final Set<String> numSplitsCbp =
        rangeContextQuery( params, new File( dataDir, cbPartDir ), separator, partPrefix );
    
    /*
    final String minTString = minT != null ? f.format( new Date( params.getMinT() ) ) : "null";
    final String maxTString = maxT != null ? f.format( new Date( params.getMaxT() ) ) : "null";
    
    System.out.printf
        ( "Range query "
                + "t=[%s,%s], "
                + "x=[%.5f,%.5f], "
                + "y=[%.5f,%.5f], "
                + "z=[%d,%d], "
                + "s=[%d,%d], "
                + "id=[%d,%d]%n",
    
            minTString,
            maxTString,
            params.getMinX(), params.getMaxX(),
            params.getMinY(), params.getMaxY(),
            params.getMinZ(), params.getMaxZ(),
            params.getMinS(), params.getMaxS(),
            params.getMinId(), params.getMaxId() );//*/
    System.out.printf( params.print( minT, maxT ) );
    
    System.out.printf( "Num splits with RAND partitioning: %d.%n", numSplitsR.size() );
    System.out.printf( "Num splits with multi-dimensional uniform grid: %d.%n", numSplitsMdg.size() );
    
    // num levels = num dimensions + 1
    final Integer[] counters = splitsPerLevel( numSplitsMlg, partPrefix, 7 );
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
