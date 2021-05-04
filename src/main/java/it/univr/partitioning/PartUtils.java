package it.univr.partitioning;

import java.io.*;
import java.util.*;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.ceil;
import static java.lang.Math.pow;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class PartUtils {

  private PartUtils() {
    // nothing here
  }

  // === Methods ===============================================================
  
  /**
   * MISSING_COMMENT
   *
   * @param input
   * @param outputDir
   */
  
  public static void generateContextBasedParts
  ( File input,
    File outputDir,
    String separator,
    String partPrefix,
    int splitSize,
    BoundariesData boundariesData )
      throws IOException {
    
    if( input == null ) {
      throw new NullPointerException();
    }
    if( outputDir == null ) {
      throw new NullPointerException();
    }
    
    if( !input.exists() || !input.isFile() ) {
      throw new IllegalArgumentException
          ( format( "The input \"%s\" is not a file.", input.getAbsolutePath() ) );
    }
    
    if( !outputDir.exists() || !outputDir.isDirectory() ) {
      outputDir.mkdirs();
    }
    
    final List<String> lines = FileUtils.readLines( input, false );
    boundariesData.computeContextBasedParts( lines, outputDir, separator, partPrefix );
    
    // --- check split dimensions ----------------------------------------------

    /*final File[] partitions = outputDir.listFiles();
    for( File p : partitions ) {
      if( p.length() > splitSize ) {
        // number of splits
        int numSplits = (int)
                ( ( p.length() / splitSize ) +
                        ( ( p.length() % splitSize > 0 ) ? 1 : 0 ) );
        int m = numSplits;
        int d = 0;
        while( m >  0 ){
          m = numSplits / 10;
          d += 1;
        }
        final String splitTemplate = format( "%%s-%%0%sd", d );
        try( BufferedReader br = new BufferedReader( new FileReader( p ) ) ) {
          long size = 0L;
          int i = 0;
          BufferedWriter bw = new BufferedWriter
                  ( new FileWriter
                          ( new File
                                  ( outputDir, format( splitTemplate, p.getName(), i ) ) ) );
          String line;
          while( ( line = br.readLine() ) != null ){
            final String value = String.format( "%s%n", line );
            if( size + value.getBytes().length > splitSize ){
              bw.close();
              bw = new BufferedWriter
                      ( new FileWriter
                              ( new File
                                      ( outputDir, format( splitTemplate, p.getName(), ++i ) ) ) );
              size = 0L;
            }
            bw.write( value );
            size += value.getBytes().length;
          }
          bw.close();
        }
        p.delete();
      }
    }//*/
  }
  
  
  /**
   * MISSING_COMMENT
   *
   * @param input
   * @param outputDir
   * @param splitSize
   * @param partPrefix
   * @throws IOException
   */

  public static void generateRandomParts
  ( File input,
    File outputDir,
    int splitSize,
    String partPrefix ) throws IOException {

    if( input == null ) {
      throw new NullPointerException();
    }
    if( outputDir == null ) {
      throw new NullPointerException();
    }
    if( partPrefix == null ) {
      throw new NullPointerException();
    }

    if( !input.exists() || !input.isFile() ) {
      throw new IllegalArgumentException
              ( format( "The input \"%s\" is not a file.", input.getAbsolutePath() ) );
    }

    if( !outputDir.exists() || !outputDir.isDirectory() ) {
      outputDir.mkdirs();
    }

    int i = 0;

    try( BufferedReader br = new BufferedReader( new FileReader( input ) ) ) {
      String line;
      long partSize = 0L;
      String partName = format( "%s%04d", partPrefix, i );
      File outFile = new File( outputDir, partName );

      BufferedWriter wr = new BufferedWriter( new FileWriter( outFile ) );

      while( ( line = br.readLine() ) != null ) {
        final String value = String.format( "%s%n", line );

        if( ( partSize + value.getBytes().length ) > splitSize ) {
          // necessary because not inside a try
          wr.close();
          partName = format( "%s%04d", partPrefix, ++i );
          outFile = new File( outputDir, partName );
          wr = new BufferedWriter( new FileWriter( outFile ) );
          partSize = 0L;
        }

        wr.write( value );
        partSize += value.getBytes().length;
      }

      // necessary because not inside a try
      wr.close();
    }
  }
  
  
  /**
   * The method generates a partitioning with a multi-dimensional uniform grid.
   *
   * @param input
   * @param outputDir
   * @param splitSize
   * @param partPrefix
   * @param boundariesData
   * @param separator
   * @throws IOException
   */

  public static void generateUniformMultiDimGridParts
  ( File input,
    File outputDir,
    int splitSize,
    String partPrefix,
    BoundariesData boundariesData,
    String separator ) throws IOException {

    if( input == null ) {
      throw new NullPointerException();
    }

    if( outputDir == null ) {
      throw new NullPointerException();
    }

    if( partPrefix == null ) {
      throw new NullPointerException();
    }

    if( boundariesData == null ) {
      throw new NullPointerException();
    }

    if( !input.exists() || !input.isFile() ) {
      throw new IllegalArgumentException
              ( format( "The input \"%s\" is not a file.", input.getAbsolutePath() ) );
    }

    if( !outputDir.exists() || !outputDir.isDirectory() ) {
      outputDir.mkdirs();
    }

    final File tempFile = new File( outputDir, "temp.csv" );

    final int numPartitions = (int) ceil( input.length() / splitSize );
    final double numDimensions = 1.0 / 4;
    final int numCellPerSide = (int) ceil( pow( numPartitions, numDimensions ));
  
    // --- assign the grid keys and save a temporary file ----------------------
  
    final Set<String> keySet = boundariesData.computeMultiDimGridParts( boundariesData, input,
        tempFile, numCellPerSide, partPrefix );
  
    // --- write the final index -----------------------------------------------

    try( BufferedReader br = new BufferedReader( new FileReader( tempFile ) ) ) {
      final Map<String, BufferedWriter> bwMap = new HashMap<>();
      final Iterator<String> it = keySet.iterator();

      while( it.hasNext() ) {
        final String k = it.next();
        bwMap.put( k, new BufferedWriter( new FileWriter( new File( outputDir, k ) ) ) );
      }

      String line;
      while( ( line = br.readLine() ) != null ) {
        final StringTokenizer tk = new StringTokenizer( line, separator );
        final String key = tk.nextToken();
        final String value = line.substring( ( key + separator ).length() );

        final BufferedWriter w = bwMap.get( key );
        w.write( format( "%s%n", value ) );
      }


      for( BufferedWriter w : bwMap.values() ) {
        w.close();
      }
    }

    // remove the temporary file
    tempFile.delete();

    // --- check split dimensions ----------------------------------------------

    final File[] partitions = outputDir.listFiles();
    for( File p : partitions ) {
      if( p.length() > splitSize ) {
        // number of splits
        int numSplits = (int)
                ( ( p.length() / splitSize ) +
                        ( ( p.length() % splitSize > 0 ) ? 1 : 0 ) );

        int m = numSplits;
        int d = 0;
        while( m >  0 ){
          m = numSplits / 10;
          d += 1;
        }
        final String splitTemplate = format( "%%s-%%0%sd", d );

        try( BufferedReader br = new BufferedReader( new FileReader( p ) ) ) {
          long size = 0L;
          int i = 0;

          BufferedWriter bw = new BufferedWriter
                  ( new FileWriter
                          ( new File
                                  ( outputDir, format( splitTemplate, p.getName(), i ) ) ) );

          String line;
          while( ( line = br.readLine() ) != null ){
            final String value = String.format( "%s%n", line );
            if( size + value.getBytes().length > splitSize ){
              bw.close();
              bw = new BufferedWriter
                      ( new FileWriter
                              ( new File
                                      ( outputDir, format( splitTemplate, p.getName(), ++i ) ) ) );
              size = 0L;
            }

            bw.write( value );
            size += value.getBytes().length;
          }

          bw.close();
        }
        p.delete();
      }
    }
  }


  /**
   * The method generates a partitioning with a multi-level uniform grid.
   *
   * @param input
   * @param outputDir
   * @param splitSize
   * @param partPrefix
   * @param boundariesData
   * @param separator
   * @throws IOException
   */

  public static void generateUniformMultiLevelGridParts
  ( File input,
    File outputDir,
    int splitSize,
    String partPrefix,
    BoundariesData boundariesData,
    String separator ) throws IOException {

    if( input == null ) {
      throw new NullPointerException();
    }

    if( outputDir == null ) {
      throw new NullPointerException();
    }

    if( partPrefix == null ) {
      throw new NullPointerException();
    }

    if( boundariesData == null ) {
      throw new NullPointerException();
    }

    if( !input.exists() || !input.isFile() ) {
      throw new IllegalArgumentException
              ( format( "The input \"%s\" is not a file.", input.getAbsolutePath() ) );
    }

    if( !outputDir.exists() || !outputDir.isDirectory() ) {
      outputDir.mkdirs();
    }

    final int numPartitions = (int) ceil( input.length() / splitSize );
    final double numDimensions = 1.0 / 4;
    final int numCellPerSide = (int) ceil( pow( numPartitions, numDimensions ));
  
    // --- assign the grid keys and save a temporary file ----------------------
  
    final List<String> lines = FileUtils.readLines( input, false );
    boundariesData.computeMultiLevelGridParts( boundariesData, lines, outputDir, numCellPerSide, partPrefix );
  
    // --- check split dimensions ----------------------------------------------

    final File[] partitions = outputDir.listFiles();
    for( File p : partitions ) {
      if( p.length() > splitSize ) {
        // number of splits
        int numSplits = (int)
                ( ( p.length() / splitSize ) +
                        ( ( p.length() % splitSize > 0 ) ? 1 : 0 ) );

        int m = numSplits;
        int d = 0;
        while( m >  0 ){
          m = numSplits / 10;
          d += 1;
        }
        final String splitTemplate = format( "%%s-%%0%sd", d );

        try( BufferedReader br = new BufferedReader( new FileReader( p ) ) ) {
          long size = 0L;
          int i = 0;

          BufferedWriter bw = new BufferedWriter
                  ( new FileWriter
                          ( new File
                                  ( outputDir, format( splitTemplate, p.getName(), i ) ) ) );

          String line;
          while( ( line = br.readLine() ) != null ){
            final String value = String.format( "%s%n", line );
            if( size + value.getBytes().length > splitSize ){
              bw.close();
              bw = new BufferedWriter
                      ( new FileWriter
                              ( new File
                                      ( outputDir, format( splitTemplate, p.getName(), ++i ) ) ) );
              size = 0L;
            }

            bw.write( value );
            size += value.getBytes().length;
          }

          bw.close();
        }
        p.delete();
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param splitKeys
   * @param partPrefix
   * @param numLevels
   * @return
   */

  public static Integer[] splitsPerLevel
  ( Set<String> splitKeys,
    String partPrefix,
    int numLevels ){

    if( partPrefix == null ) {
      throw new NullPointerException();
    }
    if( splitKeys == null ) {
      throw new NullPointerException();
    }

    final Set<String>[] splitPerLevel = new HashSet[numLevels];
    for( int i = 0; i < numLevels; i++ ){
      splitPerLevel[i] = new HashSet<>();
    }

    for( String s : splitKeys ){
      final String keys = s.replace( partPrefix, "" );
      final StringTokenizer tk = new StringTokenizer( keys, "-" );

      int i = 0;
      String combineToken = "";
      while( tk.hasMoreTokens() && i < numLevels ){
        combineToken = combineToken + tk.nextToken();
        splitPerLevel[i].add( combineToken );
        i++;
        if( tk.hasMoreTokens() ){
          combineToken =  combineToken + "-";
        }
      }
      // leaf nodes not in the final level: no splitting due only to size
      if( !tk.hasMoreTokens() && i < numLevels ){
        splitPerLevel[i].add( combineToken );
      }
    }

    final Integer[] result = new Integer[numLevels];
    for( int i = 0; i < numLevels; i++ ){
      result[i] = splitPerLevel[i].size();
    }

    return result;
  }
  
}