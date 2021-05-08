package it.univr.convert;

import it.univr.convert.parser.JSONFlat;
import it.univr.convert.writer.CSVUtils;
import it.univr.restaurant.RestaurantWritable;
//import it.univr.veronacard.VeronaCardWritable;

import java.io.File;
import java.util.*;

import static java.lang.String.format;

public class ConvertFile {
  
  // === Attributes ============================================================
  
  private static final String jsonArrayFormat = "JSONarray";
  private static final String jsonRowsFormat = "JSONrows";
  private static final String headerInFormat = "h";
  private static final String headerOutFormat = "h-1";
  
  private static final String fieldsLabel = "fields";
  
  //static final Logger LOGGER = LogManager.getLogger( ConvertFile.class );
  static final String SPLITERATOR = ",";
  
  // === Properties ============================================================
  
  private String header;
  private String jsonType;
  private File inputFile;
  private File outputFile;
  private String[] fields;
  Class<? extends CSVUtils> writerFormatClass;
  
  // === Methods ===============================================================
  
  public ConvertFile
  ( String[] args, Class<? extends CSVUtils> writerFormatClass ) {
    
    if( !checkArguments( args ) ) {
      System.exit( 0 );
    }
    
    this.writerFormatClass = writerFormatClass;
    processFieldsParameter( args[4] );
  }
  
  public static void main( String[] args ) throws Exception {
    final long start = System.currentTimeMillis();
    System.out.printf( "START: %d%n", start );
    
    /*final int res = run( new ConvertFile( args, VeronaCardCSV.class ) );//*/
    final int res = run( new ConvertFile( args, RestaurantCSV.class ) );
    
    final long end = System.currentTimeMillis();
    System.out.printf( "END: %d%n", end );
    System.out.printf( "DURATION: %d%n", ( end - start ) );
    System.exit( res );
  }
  
  public static int run( ConvertFile c ) throws Exception {
    List<Map<String, String>> flatCsv = new ArrayList<>();
    int res = 0;
    // Parse JSON file
    if( c.jsonType.equals( jsonArrayFormat ) ) {
      flatCsv = JSONFlat.parseJson( c.inputFile, true );
    } else {
      flatCsv = JSONFlat.parseJson( c.inputFile, false );
    }
    // Write CSV file
    CSVUtils csvUtils = c.writerFormatClass.newInstance();
    if( c.header.equals( headerInFormat ) ) {
      res = csvUtils.write( flatCsv, true, c.fields, c.outputFile );
    } else {
      res = csvUtils.write( flatCsv, false, c.fields, c.outputFile );
    }
    return res;
  }
  
  private boolean checkArguments( String[] args ) {
    if( args.length != 5 ) {
      System.out.printf
        ( "Invalid number of arguments: %d, required: %d.%n", args.length, 5 );
      for( int i = 0; i < args.length; i++ ) {
        System.out.printf( "args[%d] = %s.%n", i, args[i] );
      }
      printUsage();
      return false;
    }
    
    this.jsonType = args[0];
    if( !this.jsonType.equals( jsonArrayFormat ) &&
        !this.jsonType.equals( jsonRowsFormat ) ) {
      printUsage();
      return false;
    }
    
    this.header = args[1];
    if( !this.header.equals( headerInFormat ) &&
        !this.header.equals( headerOutFormat ) ) {
      printUsage();
      return false;
    }
    
    this.inputFile = new File( args[2] );
    this.outputFile = new File( args[3] );
    
    if( !checkFieldsArgument( args[4] ) ) {
      printUsage();
      return false;
    }
    return true;
  }
  
  private boolean checkFieldsArgument( String s ) {
    if( s == null || !s.startsWith(fieldsLabel) ) {
      return false;
    }
    final int start = s.indexOf("=");
    if( start == -1 ) {
      //throw new IllegalArgumentException( format( "Illegal fields specification: %s", s ) );
      System.out.println( format( "Illegal fields specification: %s", s ) );
      return false;
    }
    return true;
  }
  
  
  /**
   * The method prints an informative menu helper for using the program.
   */
    
  private void printUsage() {
    System.out.printf
        ( "Usage: Convert "
                + "<%s|%s> " // 0
                + "<%s|%s> " // 1
                + "<input_path> " // 2
                + "<output_path> " // 3
                + "<fields=-1|fields=f1,f2,...>%n", // 4
            jsonArrayFormat, jsonRowsFormat,
            headerInFormat, headerOutFormat );
  }
  
  private void processFieldsParameter( String s ) {
    s = s.replace( fieldsLabel + "=", "" );
    // Default value
    if( s.isEmpty() || s.contains( "-1" ) ) {
      // todo
      if( writerFormatClass == RestaurantCSV.class ) {
        fields = RestaurantWritable.attributes;
      } else {
        fields = null;
      }
      /*if( writerFormatClass == VeronaCardCSV.class ) {
        fields = VeronaCardWritable.attributes;
      }//*/
    } else {
      final StringTokenizer tk = new StringTokenizer( s, "," );
      this.fields = new String[ tk.countTokens() ];
      int i = 0;
      while( tk.hasMoreTokens() ) {
        final String token = tk.nextToken();
        fields[i] = token;
        i++;
      }
    }
  }
}
