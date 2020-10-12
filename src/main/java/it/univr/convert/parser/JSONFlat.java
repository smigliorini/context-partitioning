package it.univr.convert.parser;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 *  The JSONFlat class create list of key-value pairs for the generated JSON
 */
public class JSONFlat {
    static final Logger LOGGER = LogManager.getLogger(JSONFlat.class);
    private static final Class<?> JSON_OBJECT = JSONObject.class;
    private static final Class<?> JSON_ARRAY = JSONArray.class;
    public static final String CHARSET_DEFAULT = "UTF-8";

    /**
     * Parse the JSON file using the default character encoding UTF-8
     *
     * @param file      The file JSON to parse
     * @param mapping   True row-like mapping, array-like mapping otherwise
     * @return a List of key-value pairs generated from the JSON file
     */
    public static List<Map<String, String>> parseJson(File file, boolean mapping) {
        if (!mapping) return parseJsonRows(file, CHARSET_DEFAULT);
        else return parseJsonFile(file, CHARSET_DEFAULT);
    }

    /**
     * Parse the JSON file Row-like mapping using the specified character encoding
     *
     * @param file      The file JSON to parse
     * @param encoding  The character encoding
     * @param mapping   True row-like mapping, array-like mapping otherwise
     * @return a List of key-value pairs generated from the JSON file
     */
    public static List<Map<String, String>> parseJson(File file, boolean mapping, String encoding) {
        if (!mapping) return parseJsonRows(file, encoding);
        else return parseJsonFile(file, encoding);
    }

    /**
     * Parse the JSON String
     *
     * @param json  The JSON string to parse
     * @return a List of key-value pairs generated from the JSON String
     * @throws Exception Handle the JSON String as JSON Array
     */
    public static List<Map<String, String>> parseJson(String json) {
        List<Map<String, String>> flatJson = null;

        try {
            JSONObject jsonObject = new JSONObject(json);
            flatJson = new ArrayList<>();
            flatJson.add(parse(jsonObject));
        } catch (JSONException je) {
            //LOGGER.info("Handle the JSON String as JSON Array");
            flatJson = handleAsArray(json);
        }

        return flatJson;
    }

    /**
     * Parse the JSON file Row-like mapping using the specified character encoding
     *
     * @param file      The file JSON to parse
     * @param encoding  The character encoding
     * @return a List of key-value pairs generated from the JSON file
     */
    private static List<Map<String, String>> parseJsonRows(File file, String encoding) {
        List<Map<String, String>> flatJson = null;
        try {
            List<String> lines = FileUtils.readLines(file, encoding);
            flatJson = JSONFlat.parseJson(lines.toString());
        } catch (IOException e) {
            LOGGER.error("JsonFlat#ParseJsonRows(file, encoding) IOException: ", e);
        }

        return flatJson;
    }

    /**
     * Parse the JSON file using the specified character encoding
     *
     * @param file      The file JSON to parse
     * @param encoding  The character encoding
     * @return a List of key-value pairs generated from the JSON file
     */
    private static List<Map<String, String>> parseJsonFile(File file, String encoding) {
        List<Map<String, String>> flatJson = null;
        String json = "";

        try {
            json = FileUtils.readFileToString(file, encoding);
            flatJson = parseJson(json);
        } catch (IOException e) {
            LOGGER.error("JsonFlat#ParseJsonFile(file, encoding) IOException: ", e);
        } catch (Exception ex) {
            LOGGER.error("JsonFlat#ParseJsonFile(file, encoding) Exception: ", ex);
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
    private static List<Map<String, String>> handleAsArray(String json) {
        List<Map<String, String>> flatJson = null;

        try {
            JSONArray jsonArray = new JSONArray(json);
            flatJson = parse(jsonArray);
        } catch (Exception e) {
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
    private static Map<String, String> parse(JSONObject jsonObject) throws JSONException {
        Map<String, String> flatJson = new LinkedHashMap<>();
        flatten(jsonObject, flatJson, "");

        return flatJson;
    }

    /**
     * Parse a JSON Array
     *
     * @param jsonArray JSON Array
     * @return a List of key-value pairs generated from the JSON Array
     */
    private static List<Map<String, String>> parse(JSONArray jsonArray) throws JSONException {
        JSONObject jsonObject = null;
        List<Map<String, String>> flatJson = new ArrayList<>();
        int length = jsonArray.length();

        for (int i = 0; i < length; i++) {
            jsonObject = jsonArray.getJSONObject(i);
            Map<String, String> stringMap = parse(jsonObject);
            flatJson.add(stringMap);
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
    private static void flatten(JSONObject obj, Map<String, String> flatJson, String prefix) throws JSONException {
        Iterator<?> iterator = obj.keys();
        String _prefix = !prefix.equals("") ? prefix + "." : "";

        while (iterator.hasNext()) {
            String key = iterator.next().toString();

            if (obj.get(key).getClass() == JSON_OBJECT) {
                JSONObject jsonObject = (JSONObject) obj.get(key);
                flatten(jsonObject, flatJson, _prefix + key);
            } else if (obj.get(key).getClass() == JSON_ARRAY) {
                JSONArray jsonArray = (JSONArray) obj.get(key);

                if (jsonArray.length() < 1) {
                    continue;
                }

                flatten(jsonArray, flatJson, _prefix + key);
            } else {
                String value = obj.get(key).toString();

                if (value != null && !value.equals("null")) {
                    flatJson.put(_prefix + key, value);
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
    private static void flatten(JSONArray obj, Map<String, String> flatJson, String prefix) throws JSONException {
        int length = obj.length();

        for (int i = 0; i < length; i++) {
            if (obj.get(i).getClass() == JSON_ARRAY) {
                JSONArray jsonArray = (JSONArray) obj.get(i);

                // jsonArray is empty
                if (jsonArray.length() < 1) {
                    continue;
                }

                flatten(jsonArray, flatJson, prefix + '[' + i + ']');
            } else if (obj.get(i).getClass() == JSON_OBJECT) {
                JSONObject jsonObject = (JSONObject) obj.get(i);
                flatten(jsonObject, flatJson, prefix + '[' + (i + 1) + ']');
            } else {
                String value = obj.get(i).toString();

                if (value != null) {
                    flatJson.put(prefix + "[" + (i + 1) + "]", value);
                }
            }
        }
    }
}
