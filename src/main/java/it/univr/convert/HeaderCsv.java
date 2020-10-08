package it.univr.convert;

/**
 * Enum Restaurant header for CSV
 */
public enum HeaderCsv {

    BUILDING("address.building"), COORD_1("address.coord[1]"),
    COORD_2("address.coord[2]"), STREET("address.street"),
    ZIPCODE("address.zipcode"), BOROUGH("borough"),
    CUISINE("cuisine"), $DATE("date.$date"),
    GRADE("grade"), SCORE("score"),
    NAME("name"), RESTAURANT_ID("restaurant_id");

    private final String field;

    /**
     * Enum constructor
     */
    HeaderCsv(String field) { this.field = field; }

    public String getField() { return field; }
}
