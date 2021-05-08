package it.univr.convert.parser;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

/**
 *  The JSONFlat class create list of key-value pairs for the generated JSON
 */
public class JSONFlat {
  
  private static final Class<?> JSON_OBJECT = JSONObject.class;
  private static final Class<?> JSON_ARRAY = JSONArray.class;
  
  static final Logger LOGGER = LogManager.getLogger( JSONFlat.class );
  static final String CHARSET_DEFAULT = "UTF-8";
  
  // === Methods ===============================================================
  
  private JSONFlat() {
    // nothing here
  }
  
  
  /**
   * Parse the JSON file using the default character encoding UTF-8
   *
   * @param file      The file JSON to parse
   * @param mapping   True array-like mapping, row-like mapping otherwise
   * @return a List of key-value pairs generated from the JSON file
   */
  
  public static List<Map<String, String>> parseJson( File file, boolean mapping ) {
    return parseJson( file, mapping, CHARSET_DEFAULT );
  }

  
  /**
   * Parse the JSON file Row-like mapping using the specified character encoding
   *
   * @param file      The file JSON to parse
   * @param mapping   True array-like mapping, row-like mapping otherwise
   * @param encoding  The character encoding
   * @return a List of key-value pairs generated from the JSON file
   */
  
  public static List<Map<String, String>> parseJson( File file, boolean mapping, String encoding ) {
    String json = "";
    try {
      if( mapping ) {
        json = FileUtils.readFileToString( file, encoding );
      } else {
        final List<String> lines = FileUtils.readLines( file, encoding );
        json = lines.toString();
      }
    } catch( FileNotFoundException e ) {
      //System.out.printf( "File \"%s\" not found.%n", file );
      LOGGER.error( format( "File \"%s\" not found.%n", file ), e );
    } catch( IOException e ) {
      //System.out.printf( "Unable to read file \"%s\".%n", file );
      LOGGER.error( format( "Unable to read file \"%s\".%n", file ), e );
    }
    return parseJson( json );
  }
  
  
  /**
   * Parse the JSON String
   *
   * @param json  The JSON string to parse
   * @return a List of key-value pairs generated from the JSON String
   * @throws Exception Handle the JSON String as JSON Array
   */
  
  public static List<Map<String, String>> parseJson( String json ) {
    List<Map<String, String>> flatJson = new ArrayList<>();

    try {
      final JSONObject jsonObject = new JSONObject( json );
      flatJson = new ArrayList<>();
      flatJson.add( parse( jsonObject ) );
    } catch( JSONException je ) {
      //LOGGER.info( "Handle the JSON String as JSON Array" );
      flatJson = handleAsArray( json );
    }
    return flatJson;
  }

  
  /**
   * Handle the JSON String as Array
   *
   * @param json  The JSON string to parse
   * @return a List of key-value pairs generated from the JSON String
   * @throws Exception JSON might be malformed
   */
  
  private static List<Map<String, String>> handleAsArray( String json ) {
    List<Map<String, String>> flatJson = null;

    try {
      final JSONArray jsonArray = new JSONArray(json);
      flatJson = parse( jsonArray );
    } catch ( Exception e ) {
      LOGGER.error("JSON might be malformed, Please verify that your JSON is valid");
    }
    return flatJson;
  }
  
  
  /**
   * Parse a JSON Object
   *
   * @param jsonObject  JSON Object
   * @return a map of key-value pairs generated from the JSON Object
   */
  
  private static Map<String, String> parse( JSONObject jsonObject ) throws JSONException {
    final Map<String, String> flatJson = new LinkedHashMap<>();
    flatten( jsonObject, flatJson, "" );
    return flatJson;
  }

  /**
   * Parse a JSON Array
   *
   * @param jsonArray JSON Array
   * @return a List of key-value pairs generated from the JSON Array
   */
  
  private static List<Map<String, String>> parse( JSONArray jsonArray ) throws JSONException {
    final List<Map<String, String>> flatJson = new ArrayList<>();

    for( int i = 0; i < jsonArray.length(); i++ ) {
      final JSONObject jsonObject = jsonArray.getJSONObject( i );
      final Map<String, String> stringMap = parse( jsonObject );
      flatJson.add( stringMap );
    }
    return flatJson;
  }

  
  /**
   * Flatten the given JSON Object
   *
   * This method will convert the JSON object to a Map of
   * String keys and values
   *
   * @param obj       JSON Object
   * @param flatJson  The map of key-value pairs generated
   * @param prefix    prefix
   */
   
  private static void flatten
  ( JSONObject obj,
    Map<String,String> flatJson,
    String prefix )
      throws JSONException {

    if( flatJson == null ) {
      throw new NullPointerException();
    }
    
    final Iterator<?> iterator = obj.keys();
    final String _prefix = !prefix.equals("") ? prefix + "." : "";
    
    while( iterator.hasNext() ) {
      final String key = iterator.next().toString();

      if( obj.get( key ).getClass() == JSON_OBJECT ) {
        final JSONObject jsonObject = (JSONObject) obj.get( key );
        flatten( jsonObject, flatJson, _prefix + key );
      } else if( obj.get(key).getClass() == JSON_ARRAY ) {
        JSONArray jsonArray = (JSONArray) obj.get( key) ;

        if( jsonArray.length() < 1 ) {
            continue;
        }
        flatten( jsonArray, flatJson, _prefix + key );
      } else {
        final String value = obj.get(key).toString();

        if( value != null && !value.equals( "null" ) ) {
          flatJson.put( _prefix + key, value );
        }
      }
    }
  }

  
  /**
   * Flatten the given JSON Array
   *
   * @param obj       JSON Array
   * @param flatJson  The map of key-value pairs generated
   * @param prefix    prefix
   */
  
  private static void flatten
  ( JSONArray obj,
    Map<String, String> flatJson,
    String prefix )
      throws JSONException {
    
    if( flatJson == null ) {
      throw new NullPointerException();
    }
    
    for( int i = 0; i < obj.length(); i++ ) {
      if( obj.get( i ).getClass() == JSON_ARRAY ) {
        final JSONArray jsonArray = (JSONArray) obj.get( i );
        // jsonArray is empty
        if( jsonArray.length() < 1 ) {
          continue;
        }
        flatten( jsonArray, flatJson, prefix + '[' + i + ']' );
      } else if( obj.get( i ).getClass() == JSON_OBJECT ) {
        final JSONObject jsonObject = (JSONObject) obj.get( i );
        flatten( jsonObject, flatJson, prefix + '[' + ( i + 1 ) + ']' );
      } else {
        String value = obj.get( i ).toString();

        if( value != null ) {
          flatJson.put( prefix + '[' + ( i + 1 ) + ']', value );
        }
      }
    }
  }
  
}
