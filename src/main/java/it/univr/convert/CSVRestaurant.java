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
     * Enum Restaurant header for CSV
     */
    private enum Header {

        building("address.building"), coordX("address.coord[1]"), coordY("address.coord[2]"),
        street("address.street"), zipcode("address.zipcode"), borough("borough"),
        cuisine("cuisine"), $date("date.$date"), grade("grade"),
        score("score"), name("name"), restaurantId("restaurant_id");

        private final String field;

        /**
         * Enum constructor
         */
        Header(String field) { this.field = field; }
    }

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
     * @param withHeader   True with header line, without header line otherwise
     * @param headers       The header string to keep
     */
    public static void write(List<Map<String, String>> flatJson, String fileName, boolean withHeader, String[] headers) {
        writeToFile(getCSV(createRestaurants(flatJson), SPLITERATOR, withHeader, headers), fileName);
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

            restaurant.put(Header.building.name(), map.get(Header.building.field));
            restaurant.put(Header.coordX.name(), map.get(Header.coordX.field));
            restaurant.put(Header.coordY.name(), map.get(Header.coordY.field));
            restaurant.put(Header.street.name(), map.get(Header.street.field));
            restaurant.put(Header.zipcode.name(), map.get(Header.zipcode.field));

            restaurant.put(Header.borough.name(), map.get(Header.borough.field));
            restaurant.put(Header.cuisine.name(), map.get(Header.cuisine.field));

            String prefixGrades = "grades" + '[' + (i + 1) + ']' + '.';
            restaurant.put(Header.$date.name(), map.get(prefixGrades + Header.$date.field));
            restaurant.put(Header.grade.name(), map.get(prefixGrades + Header.grade.field));
            restaurant.put(Header.score.name(), map.get(prefixGrades + Header.score.field));

            restaurant.put(Header.name.name(), map.get(Header.name.field));
            restaurant.put(Header.restaurantId.name(), map.get(Header.restaurantId.field));

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
