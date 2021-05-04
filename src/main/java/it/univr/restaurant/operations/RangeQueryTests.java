package it.univr.restaurant.operations;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class RangeQueryTests {

  private static final String fileName = "rt_2000_2019_cluster.csv";

  private static double minX = -119.6368672;
  private static double minY = -28.0168595;
  private static int minZ = 10001;
  private static long minT = 1291766400000L;
  private static int minS = -1;
  private static int minId = 30075445;
  private static double maxX = 153.1628795;
  private static double maxY = 51.6514664;
  private static int maxZ = 11697;
  private static long maxT = 1421712000000L;
  private static int maxS = 131;
  private static int maxId = 40900694;

  private static double xLength = maxX - minX;
  private static double yLength = maxY - minY;
  private static double zLength = maxZ - minZ;
  private static double tLength = maxT - minT;
  private static double sLength = maxS - minS;
  private static double idLength = maxId - minId;

  private static double[] overlaps = new double[]{
    0.05,
    0.10,
    0.20,
    0.30
  };

  private static int numTests = 10;

  /**
   * MISSING_COMMENT
   *
   * @param args
   */

  public static void main( String[] args ) {
    final Random rg = new Random();
    final Path p = Paths.get( "range_query_cluster.sh" );


    try( BufferedWriter writer = Files.newBufferedWriter( p ) ) {
      writer.write( format( "#!/bin/bash%n" ) );
      writer.write( format( "export HADOOP_CLASSPATH=context_partitioning.jar%n%n" ) );

      for( double o : overlaps ) {
        final double od = Math.pow( o, 1.0 / 4.0 );

        for( int i = 0; i < numTests; i++ ) {
          final double startX = minX + rg.nextDouble() * ( xLength - xLength * od );
          final double endX = startX + xLength * od;
          final double startY = minY + rg.nextDouble() * ( yLength - yLength * od );
          final double endY = startY + yLength * od;
          final double startZ = minZ + rg.nextDouble() * ( zLength - zLength * od );
          final double endZ = startZ + zLength * od;
          final double startT = minT + rg.nextDouble() * ( tLength - tLength * od );
          final double endT = startT + tLength * od;
          final double startS = minS + rg.nextDouble() * ( sLength - sLength * od );
          final double endS = startS + sLength * od;
          //final double startId = minId + rg.nextDouble() * ( idLength - idLength * od );
          //final double endId = startId + idLength * od;


          final String rq = format( "\"Rectangle:(%f_%f_%f_%f_%f)_(%f_%f_%f_%f_%f)\" ",
                                    startX, startY, startZ, startT, startS, //startId,
                                    endX, endY, endZ, endT, endS//, endId
          );
          final String areaS = new Double( o ).toString().replace( ".", "_" );

          final StringBuilder b1 = new StringBuilder();
          b1.append( "hadoop it.univr.operations.RangeQuery " );
          b1.append( "CSVMulti 5 " );
          b1.append( rq );
          b1.append( "test/output_bc " );
          b1.append( format( "test/output_bc_rq_%s_%s ", areaS, i ) );
          b1.append( format( "> output_bc_rq_%s_%s.log 2>&1%n", areaS, i ) );
          writer.write( b1.toString() );

          final StringBuilder b2 = new StringBuilder();
          b2.append( "hadoop it.univr.operations.RangeQuery " );
          b2.append( "CSVMulti 5 " );
          b2.append( rq );
          b2.append( "test/output_md " );
          b2.append( format( "test/output_md_rq_%s_%s ", areaS, i ) );
          b2.append( format( "> output_md_rq_%s_%s.log 2>&1%n", areaS, i ) );
          writer.write( b2.toString() );

          final StringBuilder b3 = new StringBuilder();
          b3.append( "hadoop it.univr.operations.RangeQuery " );
          b3.append( "CSVMulti 5 " );
          b3.append( rq );
          b3.append( format( "test/%s ", fileName ) );
          b3.append( format( "test/output_csv_rq_%s_%s ", areaS, i ) );
          b3.append( format( "> output_csv_rq_%s_%s.log 2>&1%n", areaS, i ) );
          writer.write( b3.toString() );
        }
      }
    } catch( IOException e ) {
      e.printStackTrace();
    }
  }
}
