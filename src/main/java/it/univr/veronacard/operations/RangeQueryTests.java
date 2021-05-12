package it.univr.veronacard.operations;

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

  private static final String fileName = "vc_2000_2019_cluster_age_15_55.csv";

  private static double minX = 10.900;
  private static double minY = 45.430;
  private static long minT = 946000000000L;
  private static int minA = 0;
  private static double maxX = 11.00;
  private static double maxY = 45.450;
  private static long maxT = 1200000000000L;
  private static int maxA = 90;

  private static double xLength = maxX - minX;
  private static double yLength = maxY - minY;
  private static double tLength = maxT - minT;
  private static double aLength = maxA - minA;

  private static double[] overlaps = new double[]{
    0.05,
    0.10,
    0.15,
    0.25,
    0.30,
    0.50,
    0.75,
    // 0.95
  };

  private static int numTests = 10;

  /**
   * MISSING_COMMENT
   *
   * @param args
   */

  public static void main( String[] args ) {
    final Random rg = new Random();
    final Path p = Paths.get( "range_query_cluster_age.sh" );


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
          final double startA = minA + rg.nextDouble() * ( aLength - aLength * od );
          final double endA = startA + aLength * od;
          final double startT = minT + rg.nextDouble() * ( tLength - tLength * od );
          final double endT = startT + tLength * od;

          //final String rq = format( "\"Rectangle:(%f,%f,%f,%f)-(%f,%f,%f,%f)\" ",
          final String rq = format( "\"Rectangle:(%f %f %f %f %f)-(%f %f %f %f %f)\" ",
                                    startX, startY, startT, startA,
                                    endX, endY, endT, endA );
          final String areaS = new Double( o ).toString().replace( ".", "_" );

          final StringBuilder b1 = new StringBuilder();
          b1.append( "hadoop it.univr.operations.RangeQuery " );
          b1.append( "CSVMulti 4 " );
          b1.append( rq );
          b1.append( "test/output_bc " );
          b1.append( format( "test/output_bc_rq_%s_%s ", areaS, i ) );
          b1.append( format( "> output_bc_rq_%s_%s.log 2>&1%n", areaS, i ) );
          writer.write( b1.toString() );

          final StringBuilder b2 = new StringBuilder();
          b2.append( "hadoop it.univr.operations.RangeQuery " );
          b2.append( "CSVMulti 4 " );
          b2.append( rq );
          b2.append( "test/output_md " );
          b2.append( format( "test/output_md_rq_%s_%s ", areaS, i ) );
          b2.append( format( "> output_md_rq_%s_%s.log 2>&1%n", areaS, i ) );
          writer.write( b2.toString() );

          final StringBuilder b3 = new StringBuilder();
          b3.append( "hadoop it.univr.operations.RangeQuery " );
          b3.append( "CSVMulti 4 " );
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
