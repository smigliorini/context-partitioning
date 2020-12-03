package it.univr.convert.writer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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

    /**
     * Convert the given List of String keys-values as a CSV String with/without header line and
     * decide witch header to keep.
     *
     * @param flatJson      The List of key-value pairs generated from the JSON String
     * @param headerLine    True with header line, without header line otherwise
     * @param headers       The header list to keep
     * @return a generated CSV string
     */
    public static String getCSV( List<Map<String, String>> flatJson, boolean headerLine, String[] headers ) {
        return getCSV( flatJson, headerLine, headers );
    }

    /**
     * Convert the given List of String keys-values as a CSV String with/without headers and
     * decide witch header to keep.
     *
     * @param flatJson      The List of key-value pairs generated from the JSON String
     * @param separator     The separator can be: ',', ';' or '\t'
     * @param headerLine   True with header line, without header line otherwise
     * @param headers       The header string to keep
     * @return a generated CSV string
     */
    public static String getCSV
    ( List<Map<String, String>> flatJson,
      String separator, boolean headerLine,
      String[] headers ) {

        final List<String> fields = collectHeaders( flatJson, headers );
        final StringBuilder csvString = new StringBuilder();

        if( headerLine ) {
            csvString.append( StringUtils.join( fields, separator ) ).append("\n");
        }

        for( Map<String, String> map : flatJson ) {
            csvString.append( getSeparatedColumns( fields, map, separator ) ).append("\n");
        }

        return csvString.toString();
    }

    /**
     * Get the CSV header from selection and check if exists,
     * if you do not choose the headers all are added.
     *
     * @param flatJson The List of key-value pairs generated from the JSONObject
     * @param headers  The header list to keep
     * @return a Set of headers
     */
    private static List<String> collectHeaders( List<Map<String, String>> flatJson, String[] headers ) {
        final List<String> fields = collectHeaders( flatJson );

        if( headers.length == 0 ) {
            return fields;
        }

        List<String> fieldsToKeep = new ArrayList<>();

        for( String header : headers ) {
            if( fields.contains( header ) ) {
                fieldsToKeep.add( header );
            } else {
                LOGGER.warn( format( "The header '%s' does not exist.", header ) );
            }
        }

        return fieldsToKeep;
    }

    /**
     * Get the CSV header.
     *
     * @param flatJson The List of key-value pairs generated from the JSONObject
     * @return a Set of headers
     */
    private static List<String> collectHeaders( List<Map<String, String>> flatJson ) {
        List<String> headers = new ArrayList<>();

        for( Map<String, String> map : flatJson ) {
            headers.addAll( map.keySet() );
        }

        return headers;
    }

    /**
     * Get separated columns used a separator (comma, semi column, tab).
     *
     * @param headers   The CSV headers
     * @param map       The Map of key-value pairs contains the header and the value
     * @param separator The separator can be: ',', ';' or '\t'
     * @return a string composed of columns separated by a specific separator.
     */
    private static String getSeparatedColumns( List<String> headers, Map<String, String> map, String separator ) {
        List<String> fields = new ArrayList<>();

        for( String header : headers ) {
            final String value = map.get( header ) == null ? "" :
                    map.get( header ).replaceAll("[\\,\\;\\r\\n\\t\\s]+", " ");
            fields.add( value );
        }
        return StringUtils.join( fields, separator );
    }

    /**
     * Write the given CSV string to the given file using the default character encoding UTF-8.
     *
     * @param csvString  The csv string to write into the file
     * @param fileName   The file to write (included the path)
     */
    public static void writeToFile(String csvString, String fileName) {
        writeToFile(csvString, fileName, CHARSET_DEFAULT);
    }

    /**
     * Write the given CSV string to the given file.
     *
     * @param csvString  The csv string to write into the file
     * @param fileName   The file to write (included the path)
     * @param encoding   The character encoding
     */
    public static void writeToFile( String csvString, String fileName, String encoding ) {
        try {
            FileUtils.write( new File(fileName), csvString, encoding );
        } catch ( IOException e ) {
            LOGGER.error( "CSVWriter#writeToFile(csvString, fileName, encoding) IOException: ", e );
        }
    }
}
