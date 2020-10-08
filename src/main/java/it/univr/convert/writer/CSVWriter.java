package it.univr.convert.writer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * The CSVWriter class write the key value pairs to the specified file.
 */
public class CSVWriter {

    static final Logger LOGGER = LogManager.getLogger(CSVWriter.class);
    public static final String CHARSET_DEFAULT = "UTF-8";

    /**
     * Convert the given List of String keys-values as a CSV String without header line and
     * decide witch header to keep.
     *
     * @param flatJson  The List of key-value pairs generated from the JSON String
     * @param headers   The header string to keep
     * @return a generated CSV string
     */
    public static String getCSV(List<Map<String, String>> flatJson, String[] headers) {
        return getCSV(flatJson, false, headers);
    }

    /**
     * Convert the given List of String keys-values as a CSV String with/without header line and
     * decide witch header to keep.
     *
     * @param flatJson      The List of key-value pairs generated from the JSON String
     * @param withHeaders   True with header line, without header line otherwise
     * @param headers       The header string to keep
     * @return a generated CSV string
     */
    public static String getCSV(List<Map<String, String>> flatJson, boolean withHeaders, String[] headers) {
        return getCSV(flatJson, withHeaders, headers);
    }

    /**
     * Convert the given List of String keys-values as a CSV String with/without headers and
     * decide witch header to keep.
     *
     * @param flatJson      The List of key-value pairs generated from the JSON String
     * @param separator     The separator can be: ',', ';' or '\t'
     * @param withHeaders   True with header line, without header line otherwise
     * @param headers       The header string to keep
     * @return a generated CSV string
     */
    public static String getCSV(List<Map<String, String>> flatJson, String separator, boolean withHeaders, String[] headers) {
        Set<String> headersSet = collectHeaders(flatJson, headers);
        StringBuilder csvString = new StringBuilder();

        if (withHeaders) {
            csvString = new StringBuilder(StringUtils.join(headersSet.toArray(), separator) + "\n");
        }

        for (Map<String, String> map : flatJson) {
            csvString.append(getSeparatedColumns(headersSet, map, separator)).append("\n");
        }

        return csvString.toString();
    }

    /**
     * Get the CSV header from selection and check if exists.
     *
     * @param flatJson The List of key-value pairs generated from the JSONObject
     * @param headers  The header string to keep
     * @return a Set of headers
     */
    private static Set<String> collectHeaders(List<Map<String, String>> flatJson, String[] headers) {
        Set<String> set = collectHeaders(flatJson);
        Set<String> headersSet = new LinkedHashSet<>();

        if (headers.length == 0) {
            headersSet = set;
        } else {
            for (String header : headers) {

                if (set.contains(header)) {
                    headersSet.add(header);
                } else {
                    LOGGER.warn("The header '" + header + "' not exists");
                }
            }
        }

        return headersSet;
    }

    /**
     * Get the CSV header.
     *
     * @param flatJson The List of key-value pairs generated from the JSONObject
     * @return a Set of headers
     */
    private static Set<String> collectHeaders(List<Map<String, String>> flatJson) {
        Set<String> headersSet = new LinkedHashSet<>();

        for (Map<String, String> map : flatJson) {
            headersSet.addAll(map.keySet());
        }

        return headersSet;
    }

    /**
     * Get separated columns used a separator (comma, semi column, tab).
     *
     * @param headers   The CSV headers
     * @param map       The Map of key-value pairs contains the header and the value
     * @param separator The separator can be: ',', ';' or '\t'
     * @return a string composed of columns separated by a specific separator.
     */
    private static String getSeparatedColumns(Set<String> headers, Map<String, String> map, String separator) {
        List<String> items = new ArrayList<>();
        for (String header : headers) {
            String value = map.get(header) == null ? "" : map.get(header).replaceAll("[\\,\\;\\r\\n\\t\\s]+", " ");
            items.add(value);
        }

        return StringUtils.join(items.toArray(), separator);
    }

    /**
     * Write the given CSV string to the given file using the default character encoding UTF-8.
     *
     * @param csvString  The csv string to write into the file
     * @param fileName   The file to write (included the path)
     */
    public static void writeToFile(String csvString, String fileName) {
        try {
            FileUtils.write(new File(fileName), csvString, CHARSET_DEFAULT);
        } catch (IOException e) {
            LOGGER.error("CSVWriter#writeToFile(csvString, fileName) IOException: ", e);
        }
    }

    /**
     * Write the given CSV string to the given file.
     *
     * @param csvString  The csv string to write into the file
     * @param fileName   The file to write (included the path)
     * @param encoding   The character encoding
     */
    public static void writeToFile(String csvString, String fileName, String encoding) {
        try {
            FileUtils.write(new File(fileName), csvString, encoding);
        } catch (IOException e) {
            LOGGER.error("CSVWriter#writeToFile(csvString, fileName, encoding) IOException: ", e);
        }
    }
}
