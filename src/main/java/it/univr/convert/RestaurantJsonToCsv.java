package it.univr.convert;

import it.univr.convert.parser.JSONFlat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RestaurantJsonToCsv {
    static final Logger LOGGER = LogManager.getLogger(RestaurantJsonToCsv.class);

    public static void main (String... args) {

        if (args.length == 0) {
            LOGGER.error( "No input file." );
            System.exit( 1 );
        }
        runJsonToCsv(args);
    }

    /**
     * Convert Rows-like mapping JSON file to CSV file without header line and ',' separator
     * @param args
     */
    // TODO
    public static void runJsonToCsv(String ... args) {
        String jsonPath = Paths.get(args[0]).toString();
        String csvPath = jsonPath.replace(".json", ".csv");

        List<Map<String, String>> list = JSONFlat.parseJson(new File(jsonPath));
        // args header to keep
        String[] headers = Arrays.copyOfRange(args, 1, args.length);
        CSVRestaurant.write(list, csvPath, headers);
    }
}
