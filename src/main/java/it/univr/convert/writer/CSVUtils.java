package it.univr.convert.writer;

import java.io.File;
import java.util.List;
import java.util.Map;

import static it.univr.convert.writer.CSVWriter.writeFile;
import static it.univr.convert.writer.CSVWriter.getCSV;

public abstract class CSVUtils {
  
  //static final Logger LOGGER = LogManager.getLogger( CSVUtils.class );
  
  public CSVUtils() {
    // nothing here
  }
    
  // === Methods ===============================================================
  
  /**
   * Convert the given List of restaurant as a single grades and write to the given file without header line
   * and decide witch header to keep.
   * @param flatJson  The List of restaurant to convert
   * @param header    True with header line, without header line otherwise
   * @param fields    The header string to keep
   * @param file      The file to write
   */
   
  public int write( List<Map<String, String>> flatJson, boolean header, String[] fields, File file ) {
    return write( flatJson, CSVWriter.SPLITERATOR, header, fields, file, CSVWriter.CHARSET_DEFAULT );
  }
  
  
  /**
   * Convert the given List of restaurant as a single grades and write to the given file with/without header line
   * and decide witch header to keep.
   *
   * @param flatJson   The List of restaurant to convert
   * @param separator  The separator can be: ',', ';' or '\t'
   * @param header     True with header line, without header line otherwise
   * @param fields     The header string to keep
   * @param file       The file to write
   * @param encoding   The character encoding
   */
   
  public int write
  ( List<Map<String, String>> flatJson,
    String separator,
    boolean header,
    String[] fields,
    File file,
    String encoding ) {
    
    return writeFile(
        getCSV( parseRecords(flatJson), separator, header, fields ),
        file,
        encoding
    );
  }
  
  
  /**
   * @param list The List of String keys-values from JSON
   * @return
   */
  
  public abstract List<Map<String, String>> parseRecords( List<Map<String, String>> list );
  
  
  /**
   * Get the number of elements inside an array
   *
   * @param fields  The headers from map keys-values
   * @param prefix  The array prefix
   * @return a count of values in the array
   */
  
  public static int getArraySize( String fields, String prefix ) {
    int count = 0;
    boolean check = true;

    while( check ) {
      count++;
      check = fields.contains( prefix + '[' + count + ']' );
    }

    return count-1;
  }
}
