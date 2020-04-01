package it.univr.partitioning;

import it.univr.veronacard.VeronaCardRecord;

import java.io.*;
import java.util.*;

import static java.lang.Double.*;
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
    double[] timeSplits,
    double[] xSplits,
    double[] ySplits,
    double[] ageSplits ) throws IOException {

    if( input == null ){
      throw new NullPointerException();
    }
    if( outputDir == null ){
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
    final List<VeronaCardRecord> records = DataUtils.parseRecords( lines, separator );
    final String partTemplate = "%s%s-%s-%s-%s";
    final Map<String,List<VeronaCardRecord>> result = new HashMap<>();

    for( VeronaCardRecord r : records ){
      int t = -1;
      int i = 1;
      while( i < timeSplits.length && t == - 1 ){
        if( r.getTime() >= timeSplits[i-1] && r.getTime() <= timeSplits[i] ){
          t = i-1;
        }
        i++;
      }
      if( t == -1 ){
        throw new IllegalStateException();
      }

      int x = -1;
      i = 1;
      while( i < xSplits.length && x == - 1 ){
        if( r.getX() >= xSplits[i-1] && r.getX() <= xSplits[i] ){
          x = i-1;
        }
        i++;
      }
      if( x == -1 ){
        throw new IllegalStateException();
      }

      int y = -1;
      i = 1;
      while( i < ySplits.length && y == - 1 ){
        if( r.getY() >= ySplits[i-1] && r.getY() <= ySplits[i] ){
          y = i-1;
        }
        i++;
      }
      if( y == -1 ){
        throw new IllegalStateException();
      }

      int age = -1;
      i = 1;
      while( i < ageSplits.length && age == - 1 ){
        if( r.getAge() >= ageSplits[i-1] && r.getAge() <= ageSplits[i] ){
          age = i-1;
        }
        i++;
      }
      if( age == -1 ){
        throw new IllegalStateException();
      }

      final String key = format( partTemplate, partPrefix, t, x, y, age );
      List<VeronaCardRecord> list = result.get( key );
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add( r );
      result.put( key, list );
    }

    // --- write the final index -----------------------------------------------

    for( String k : result.keySet() ){
      try(  BufferedWriter bw = new BufferedWriter( new FileWriter( new File( outputDir, k ) ) ) ) {
        for( VeronaCardRecord r : result.get(k )) {
          bw.write(format("%s%n", r.toString( separator )));
        }
      }
    }

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
   * @param boundaries
   * @param separator
   * @throws IOException
   */

  public static void generateUniformMultiDimGridParts
  ( File input,
    File outputDir,
    int splitSize,
    String partPrefix,
    Boundaries boundaries,
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

    if( boundaries == null ) {
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

    final double widthSidePartX = ( boundaries.getMaxX() - boundaries.getMinX() ) / numCellPerSide;
    final double widthSidePartY = ( boundaries.getMaxY() - boundaries.getMinY() ) / numCellPerSide;
    final double widthSidePartT = ((double)( boundaries.getMaxT() - boundaries.getMinT() )) / numCellPerSide;
    final double widthSidePartA = ((double)( boundaries.getMaxAge() - boundaries.getMinAge() )) / numCellPerSide;

    final String xFormat = String.format( "%%0%sd", numCellPerSide );
    final String yFormat = String.format( "%%0%sd", numCellPerSide );
    final String tFormat = String.format( "%%0%sd", numCellPerSide );
    final String aFormat = String.format( "%%0%sd", numCellPerSide );

    final String partTemplate =
      partPrefix +
      xFormat + "-" + yFormat + "-" +
      tFormat + "-" + aFormat;

    // --- assign the grid keys and save a temporary file ----------------------

    final Set<String> keySet = new HashSet<String>();

    try( BufferedReader br = new BufferedReader( new FileReader( input ) ) ) {
        try( BufferedWriter wr = new BufferedWriter( new FileWriter( tempFile ) ) ) {

        String line;
        while( ( line = br.readLine() ) != null ) {
          final VeronaCardRecord r = DataUtils.parseRecord( line, separator );

          // the cell index is given by the integer part except for the max boundary
          final int xPart;
          if( r.getX() == boundaries.getMaxX() ){
            xPart = numCellPerSide - 1;
          } else {
            xPart = (int) ( ( r.getX() - boundaries.getMinX() ) / widthSidePartX );
          }

          // the cell index is given by the integer part except for the max boundary
          final int yPart;
          if( r.getY() == boundaries.getMaxY() ){
            yPart = numCellPerSide - 1;
          } else {
            yPart = (int) ( ( r.getY() - boundaries.getMinY() ) / widthSidePartY );
          }

          // the cell index is given by the integer part except for the max boundary
          final int tPart;
          if( r.getTime() == boundaries.getMaxT() ){
            tPart = numCellPerSide - 1;
          } else {
            tPart = (int) ( ( r.getTime() - boundaries.getMinT() ) / widthSidePartT );
          }

          // the cell index is given by the integer part except for the max boundary
          final int aPart;
          if( r.getAge() == boundaries.getMaxAge() ){
            aPart = numCellPerSide - 1;
          } else {
            aPart = (int) ( ( r.getAge() - boundaries.getMinAge() ) / widthSidePartA );
          }

          final String key = format( partTemplate, xPart, yPart, tPart, aPart );
          keySet.add( key );

          wr.write( format( "%s,%s%n", key, line ) );//write in the file the entire kei and value.
        }
      }
    }

    // --- write the final index -----------------------------------------------

    try( BufferedReader br = new BufferedReader( new FileReader( tempFile ) ) ) { //tempFiles contains the indexes
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
        int d = 0; //number of digits necessary
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
   * @param boundaries
   * @param separator
   * @throws IOException
   */

  public static void generateUniformMultiLevelGridParts
  ( File input,
    File outputDir,
    int splitSize,
    String partPrefix,
    Boundaries boundaries,
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

    if( boundaries == null ) {
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

    final String tFormat = String.format( "%%0%sd", (int) Math.ceil( numCellPerSide ) );
    final String xFormat = String.format( "%%0%sd", (int) Math.ceil( numCellPerSide ) );
    final String yFormat = String.format( "%%0%sd", (int) Math.ceil( numCellPerSide ) );
    final String aFormat = String.format( "%%0%sd", (int) Math.ceil( numCellPerSide ) );

    // --- assign the grid keys and save a temporary file ----------------------

    final List<String> lines = FileUtils.readLines( input, false );
    final List<VeronaCardRecord> records = DataUtils.parseRecords( lines, separator );

    // first level => time
    final double widthSidePartT = ((double)( boundaries.getMaxT() - boundaries.getMinT() )) / numCellPerSide;
    final String tPartTemplate = partPrefix + tFormat;
    final Map<String,List<VeronaCardRecord>> tParts = new HashMap<>();

    for( VeronaCardRecord r : records ){
      if( r.getTime() != null ) {
        // the cell index is given by the integer part except for the max boundary
        final int tPart;
        if( r.getTime() == boundaries.getMaxT() ){
          tPart = numCellPerSide - 1;
        } else {
          tPart = (int) ( ( r.getTime() - boundaries.getMinT() ) / widthSidePartT );
        }
        List<VeronaCardRecord> elements = tParts.get( format( tPartTemplate, tPart ) );
        if( elements == null ){
          elements = new ArrayList<>();
        }
        elements.add( r );
        tParts.put( format( tPartTemplate, tPart ), elements );
      }
    }

    // second level => x
    final String xPartTemplate =  "%s-" + xFormat;
    final Map<String,List<VeronaCardRecord>> xParts = new HashMap<>();

    for( String k : tParts.keySet() ) {
      double minX = MAX_VALUE;
      double maxX = MIN_VALUE;

      // find the x-boundary of the t-split
      for( VeronaCardRecord r : tParts.get(k) ) {
        minX = min(minX, r.getX());
        maxX = max(maxX, r.getX());
      }//*/

      //double maxX = boundaries.getMaxX();
      //double minX = boundaries.getMinX();

      final double widthSidePartX = ( maxX - minX ) / numCellPerSide;

      for( VeronaCardRecord r : tParts.get( k )) {
        // for each temporal split
        if (r.getX() != null) {
          // the cell index is given by the integer part except for the max boundary
          final int xPart;
          //if( r.getX().equals( boundaries.getMaxX() )){
          if( r.getX() == maxX ){
            xPart = numCellPerSide - 1;
          } else {
            xPart = (int) ((r.getX() - minX) / widthSidePartX);
          }

          List<VeronaCardRecord> elements = xParts.get(format(xPartTemplate, k, xPart));
          if (elements == null) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          xParts.put(format(xPartTemplate, k, xPart), elements);
        }
      }
    }

    // third level => y
    final String yPartTemplate =  "%s-" + yFormat;
    final Map<String,List<VeronaCardRecord>> yParts = new HashMap<>();

    for( String k : xParts.keySet() ) {
      double minY = MAX_VALUE;
      double maxY = MIN_VALUE;

      // find the x-boundary of the t-split
      for( VeronaCardRecord r : xParts.get(k) ) {
        minY = min(minY, r.getY());
        maxY = max(maxY, r.getY());
      }//*/

      //final double minY = boundaries.getMinY();
      //final double maxY = boundaries.getMaxY();

      final double widthSidePartY = ( maxY - minY ) / numCellPerSide;

      for( VeronaCardRecord r : xParts.get( k )) {
        // for each temporal split
        if (r.getY() != null) {
          // int yPart = (int) ((r.getY() - minY) / widthSidePartY);
          // the cell index is given by the integer part except for the max boundary
          final int yPart;
          //if( r.getY().equals( boundaries.getMaxY() )){
          if( r.getY() == maxY ){
            yPart = numCellPerSide - 1;
          } else {
            yPart = (int) ((r.getY() - minY) / widthSidePartY);
          }

          List<VeronaCardRecord> elements = yParts.get(format(yPartTemplate, k, yPart));
          if (elements == null) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          yParts.put(format(yPartTemplate, k, yPart), elements);
        }
      }
    }

    // fourth level => age
    final String aPartTemplate =  "%s-" + aFormat;
    final Map<String,List<VeronaCardRecord>> aParts = new HashMap<>();

    for( String k : yParts.keySet() ) {
      double minA = MAX_VALUE;
      double maxA = MIN_VALUE;

      // find the x-boundary of the t-split
      for( VeronaCardRecord r : yParts.get(k) ) {
        minA = min(minA, r.getAge());
        maxA = max(maxA, r.getAge());
      }//*/

      //final double minA = boundaries.getMinAge();
      //final double maxA = boundaries.getMaxAge();

      final double widthSidePartA = ( maxA - minA ) / numCellPerSide;

      for( VeronaCardRecord r : yParts.get( k )) {
        // for each temporal split
        if (r.getAge() != null) {
          //int aPart = (int) ((r.getAge() - minA) / widthSidePartA );
          // the cell index is given by the integer part except for the max boundary
          final int aPart;
          //if( r.getAge().equals( boundaries.getMaxAge() )){
          if( r.getAge() == maxA ){
            aPart = numCellPerSide - 1;
          } else {
            aPart = (int) ((r.getAge() - minA) / widthSidePartA );
          }

          List<VeronaCardRecord> elements = aParts.get(format(aPartTemplate, k, aPart));
          if (elements == null) {
            elements = new ArrayList<>();
          }
          elements.add(r);
          aParts.put(format(aPartTemplate, k, aPart), elements);
        }
      }
    }

    // --- write the final index -----------------------------------------------
    for( String k : aParts.keySet() ){
      try(  BufferedWriter bw = new BufferedWriter( new FileWriter( new File( outputDir, k ) ) ) ) {
        for( VeronaCardRecord r : aParts.get(k )) {
          bw.write(format("%s%n", r.toString( separator )));
        }
      }
    }

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
