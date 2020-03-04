package it.univr.veronacard.partitioning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class FileUtils {

  private static final long kbyte = 1024;
  private static final long mbyte = 1024 * 1024;
  private static final long gbyte = 1024 * 1024 * 1024;

  private FileUtils(){
    // nothing here
  }

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param bytes
   */

  public static void printSize( long bytes ) {
    if( bytes / kbyte > 0 ) {
      if( bytes / mbyte > 0 ) {
        if( bytes / gbyte > 0 ) {
          System.out.printf
            ( "Input file dimension: %.2f Gbytes.%n",
              (double) bytes / gbyte );
        } else {
          System.out.printf
            ( "Input file dimension: %.2f Mbytes.%n",
              (double) bytes / mbyte );
        }
      } else {
        System.out.printf
          ( "Input file dimension: %.2f Kbytes.%n",
            (double) bytes / kbyte );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @return
   */

  public static List<String> readLines( File file, boolean header ) {
    if( file == null ) {
      throw new NullPointerException();
    }

    final List<String> lines = new ArrayList<>();

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      String line;
      int lineCount = 0;

      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {
          lines.add( line );
        }
        lineCount++;
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    }

    return lines;
  }
}
