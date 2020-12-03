package it.univr.convert;

import it.univr.convert.parser.JSONFlat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class RestaurantJsonToCsv {
    static final Logger LOGGER = LogManager.getLogger( RestaurantJsonToCsv.class );

    public static void main( String... args ) {
        runJsonToCsv( args );
    }

    public static void runJsonToCsv( String ... args ) {
        String pathJson, pathCsv;
        String[] headers = new String[0];

        if ( args.length >= 1 ) {
            // TODO
            if ( args.length == 1 ) {
                pathJson = args[0];
                pathCsv = pathJson.replace( ".json", ".csv" );
            } else {
                pathJson = args[0];
                pathCsv = args[1];
            }
            // args header to keep
            if ( args.length > 2 ) {
                headers = Arrays.copyOfRange( args, 2, args.length );
            }
            // Convert row-like mapping JSON without header line
            JsonToCsv( pathJson, pathCsv, headers );
        } else {
            LOGGER.error( format( "Invalid number of parameters: %d.", args.length ) );
        }
    }

    /**
     * Convert Row-like mapping JSON file to CSV file without header line and ',' separator
     * @param pathCsv       The file to read (included the path)
     * @param pathJson      The file to write (included the path)
     * @param headers       The header string to keep
     */
    public static void JsonToCsv( String pathJson, String pathCsv, String[] headers ) {
        JsonToCsv( pathJson, pathCsv, false, false, headers );
    }

    /**
     * Convert JSON file to CSV file with/without header line and ',' separator
     * @param pathCsv       The file to read (included the path)
     * @param pathJson      The file to write (included the path)
     * @param mapping       True array-like mapping, row-like mapping otherwise
     * @param headerLine    True with header line, without header line otherwise
     * @param headers       The header string to keep
     */
    public static void JsonToCsv
    ( String pathJson,
      String pathCsv,
      boolean mapping,
      boolean headerLine,
      String[] headers ) {

        // Parse JSON file
        final List<Map<String, String>> flatCsv = JSONFlat.parseJson( new File( pathJson ), mapping );
        // Write CSV file
        CSVRestaurant.write( flatCsv, pathCsv, headerLine, headers );
    }
}
