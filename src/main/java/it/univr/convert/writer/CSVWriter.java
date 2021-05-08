package it.univr.convert.writer;

import org.apache.commons.io.FileUtils;
//import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.lang.String.format;


/**
 * The CSVWriter class write the key value pairs to the specified file.
 */
public class CSVWriter {

  static final Logger LOGGER = LogManager.getLogger( CSVWriter.class );
  public static final String CHARSET_DEFAULT = "UTF-8";
  public static final String SPLITERATOR = ",";
  
  // === Methods ===============================================================
  
  private CSVWriter() {
    // nothing here
  }
  
  
  /**
   * Write the given CSV string to the given file.
   *
   * @param csvString  The csv string to write into the file
   * @param file       The file to write
   */
  
  public static int writeFile( String csvString, File file ) {
    return writeFile( csvString, file, CHARSET_DEFAULT );
  }
  
  
  /**
   * Write the given CSV string to the given file.
   *
   * @param csvString  The csv string to write into the file
   * @param file       The file to write
   * @param encoding   The character encoding
   */
  
  public static int writeFile( String csvString, File file, String encoding ) {
    try {
      FileUtils.write( file, csvString, encoding );
    } catch( IOException e ) {
      LOGGER.error( format( "Unable to read file \"%s\".%n", file ), e );
      return 1;
    }
    return 0;
  }
  
  
  /**
   * Convert the given List of String keys-values as a CSV String with/without header line and
   * decide witch fileds to keep.
   *
   * @param flatJson   The List of key-value pairs
   * @param separator  The separator can be: ',', ';' or '\t'
   * @param header     True with header line, without header line otherwise
   * @param fields     The fields to keep
   * @return a generated CSV string
   */
  
  public static String getCSV
  ( List<Map<String, String>> flatJson,
    String separator, boolean header, String[] fields ) {
    
    if( flatJson == null ) {
      throw new NullPointerException();
    }
    
    final List<String> f = collectHeaders( flatJson, fields );
    final StringBuilder csvString = new StringBuilder();

    if( header )
      csvString.append( String.join( separator, f ) ).append( "\n" );
    
    for( Map<String, String> map : flatJson )
      csvString.append( getSeparatedColumns( f, map, separator ) ).append( "\n" );
    
    return csvString.toString();
  }

  
  /**
   * Get the CSV header from selection and check if exists,
   * if you do not choose the headers all are added.
   *
   * @param flatJson The List of key-value pairs
   * @param fields   The fields list to keep
   * @return a list of headers
   */
  // todo: migliorare
  private static List<String> collectHeaders
  ( List<Map<String, String>> flatJson, String[] fields ) {
    
    if( flatJson == null ) {
      throw new NullPointerException();
    }
  
    final List<String> allFields = collectHeaders( flatJson );
    // Default value
    if( fields == null || fields.length == 0 ) {
      return allFields;
    }

    final List<String> keep = new ArrayList<>();
    for( String f : fields ) {
      if( allFields.contains(f) ) {
        keep.add(f);
      } else {
        LOGGER.warn( format( "The field \"%s\" does not exist.%n", f ) );
      }
    }
    return keep;
  }

  
  /**
   * Get the CSV header.
   *
   * @param flatJson The List of key-value pairs
   * @return a list of headers
   */
  
  private static List<String> collectHeaders( List<Map<String, String>> flatJson ) {
    
    if( flatJson == null ) {
      throw new NullPointerException();
    }
    
    final List<String> fields = new ArrayList<>();
    for( Map<String, String> map : flatJson ) {
      fields.addAll( map.keySet() );
    }
    return fields;
  }

  
  /**
   * Get separated columns used a separator (comma, semi column, tab).
   *
   * @param fields    The CSV headers
   * @param map       The Map of key-value pairs contains the header and the value
   * @param separator The separator can be: ',', ';' or '\t'
   * @return a string composed of columns separated by a specific separator.
   */
  
  private static String getSeparatedColumns
  ( List<String> fields, Map<String, String> map, String separator ) {
    
    if( fields == null ) {
      throw new NullPointerException();
    }
    if( map == null ) {
      throw new NullPointerException();
    }
    
    final List<String> keep = new ArrayList<>();
    for( String header : fields ) {
      final String value = map.get( header ) == null ? "" :
      map.get( header ).replaceAll("[\\,\\;\\r\\n\\t\\s]+", " ");
      keep.add( value );
    }
    return String.join( separator, keep );
  }
}
