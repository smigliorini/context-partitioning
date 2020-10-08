package it.univr.convert;

import it.univr.convert.writer.CSVWriter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The CSVRestaurant class convert the list of key-value pairs to the pojo format
 */
public class CSVRestaurant extends CSVWriter {
    static final Logger LOGGER = LogManager.getLogger(CSVRestaurant.class);
    public static final String SPLITERATOR = ",";

    /**
     * Convert the given List of restaurant as a single grades and write to the given file without header line
     * and decide witch header to keep.
     *
     * @param flatJson  The List of restaurant to convert
     * @param fileName  The file to write (included the path)
     * @param headers   The header string to keep
     */
    public static void write(List<Map<String, String>> flatJson, String fileName, String[] headers) {
        writeToFile(getCSV(createRestaurants(flatJson), SPLITERATOR, false, headers), fileName);
    }

    /**
     * Convert the given List of restaurant as a single grades and write to the given file with/without header line
     * and decide witch header to keep.
     *
     * @param flatJson      The List of restaurant to convert
     * @param fileName      The file to write (included the path)
     * @param withHeaders   True with header line, without header line otherwise
     * @param headers       The header string to keep
     */
    public static void write(List<Map<String, String>> flatJson, String fileName, boolean withHeaders, String[] headers) {
        writeToFile(getCSV(createRestaurants(flatJson), SPLITERATOR, withHeaders, headers), fileName);
    }

    /**
     * Convert the given List of restaurants as a single grades object for line.
     *
     * @param flatJson The List of String keys-values from JSON
     * @return a List of keys-values with single grades for line
     */
    public static List<Map<String, String>> createRestaurants(List<Map<String, String>> flatJson) {
        List<Map<String, String>> restaurants = new ArrayList<>();

        for (Map<String, String> map : flatJson) {
            restaurants.addAll(flattenGrades(map));
        }

        return restaurants;
    }

    /**
     * Convert the given restaurant as a List of restaurants with a single grade for line.
     *
     * @param map The string keys-values from JSON
     * @return a List of keys-values with single grade
     */
    private static List<Map<String, String>> flattenGrades(Map<String, String> map) {
        List<Map<String, String>> flatGrades = new ArrayList<>();

        for (int i = 0; i < getArraySize(map.entrySet().toString(), "grades"); i++) {
            Map<String, String> restaurant = new LinkedHashMap<>();

            restaurant.put(HeaderCsv.BUILDING.getField(), map.get(HeaderCsv.BUILDING.getField()));
            restaurant.put(HeaderCsv.COORD_1.getField(), map.get(HeaderCsv.COORD_1.getField()));
            restaurant.put(HeaderCsv.COORD_2.getField(), map.get(HeaderCsv.COORD_2.getField()));
            restaurant.put(HeaderCsv.STREET.getField(), map.get(HeaderCsv.STREET.getField()));
            restaurant.put(HeaderCsv.ZIPCODE.getField(), map.get(HeaderCsv.ZIPCODE.getField()));

            restaurant.put(HeaderCsv.BOROUGH.getField(), map.get(HeaderCsv.BOROUGH.getField()));
            restaurant.put(HeaderCsv.CUISINE.getField(), map.get(HeaderCsv.CUISINE.getField()));

            String prefixGrades = "grades" + '[' + (i + 1) + ']' + '.';
            restaurant.put(HeaderCsv.$DATE.getField(), map.get(prefixGrades + HeaderCsv.$DATE.getField()));
            restaurant.put(HeaderCsv.GRADE.getField(), map.get(prefixGrades + HeaderCsv.GRADE.getField()));
            restaurant.put(HeaderCsv.SCORE.getField(), map.get(prefixGrades + HeaderCsv.SCORE.getField()));

            restaurant.put(HeaderCsv.NAME.getField(), map.get(HeaderCsv.NAME.getField()));
            restaurant.put(HeaderCsv.RESTAURANT_ID.getField(), map.get(HeaderCsv.RESTAURANT_ID.getField()));

            flatGrades.add(restaurant);
        }

        return flatGrades;
    }

    /**
     * Get the number of elements inside an array
     *
     * @param headers   The headers from map keys-values
     * @param prefix    The array prefix
     * @return a count of values in the array
     */
    private static int getArraySize(String headers , String prefix) {
        int count = 0;
        boolean check = true;

        while(check) {
            count++;
            check = headers.contains(prefix + '[' + count + ']');
        }

        return count -1;
    }
}
