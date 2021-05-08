package it.univr.convert;

import it.univr.convert.writer.CSVUtils;
import it.univr.restaurant.RestaurantWritable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The CSVRestaurant class convert the list of key-value pairs to the pojo format
 */
public class RestaurantCSV
    extends CSVUtils {
  
  /**
   * Enum Restaurant header for CSV
   */
  /*
  private enum Header {
    building("address.building"),
    coordX("address.coord[1]"),
    coordY("address.coord[2]"),
    street("address.street"),
    zipcode("address.zipcode"),
    borough("borough"),
    cuisine("cuisine"),
    time("date.$date"),
    grade("grade"),
    score("score"),
    name("name"),
    restaurantId("restaurant_id");

    private final String field;
    Header( String field ) { this.field = field; }
  }//*/
  
  /**
   * Fields of the JSON file
   */
  
  public static final String[] attributes = {
      // "address.coord[1]", "address.coord[2]", "address.zipcode", "date.$date", "score", "restaurant_id"
      "address.building",
      "address.coord[1]",
      "address.coord[2]",
      "address.street",
      "address.zipcode",
      "borough",
      "cuisine",
      "date.$date",
      "grade",
      "score",
      "name",
      "restaurant_id"
  };//*/
  
  // === Methods ===============================================================
  
  public RestaurantCSV() {
    // nothing here
  }
  
  
  /**
   * Convert the given List of restaurants as a single grades object for line.
   *
   * @param flatJson The List of String keys-values from JSON
   * @return a List of keys-values with single grades for line
   */
    
  public List<Map<String, String>> parseRecords( List<Map<String, String>> flatJson ) {
    
    if( flatJson == null ) {
      throw new NullPointerException();
    }
    
    final List<Map<String, String>> restaurants = new ArrayList<>();

    for( Map<String, String> map : flatJson ) {
      restaurants.addAll( parseRecord( map ) );
    }

    return restaurants;
  }

  
  /**
   * Convert the given restaurant as a List of restaurants with a single grade for line.
   *
   * @param map The string keys-values from JSON
   * @return a List of keys-values with single grade
   */
    
  private static List<Map<String, String>> parseRecord( Map<String, String> map ) {
    
    if( map == null ) {
      throw new NullPointerException();
    }
    
    final List<Map<String, String>> flatGrades = new ArrayList<>();
    final int size = getArraySize( map.entrySet().toString(), "grades" );
    
    for( int i = 0; i < size; i++ ) {
      final Map<String, String> r = new LinkedHashMap<>();
      
      /*r.put( Header.building.name(), map.get(Header.building.field) );
      r.put( Header.coordX.name(), map.get(Header.coordX.field) );
      r.put( Header.coordY.name(), map.get(Header.coordY.field) );
      r.put( Header.street.name(), map.get(Header.street.field) );
      r.put( Header.zipcode.name(), map.get(Header.zipcode.field) );

      r.put( Header.borough.name(), map.get(Header.borough.field) );
      r.put( Header.cuisine.name(), map.get(Header.cuisine.field) );

      final String prefixGrades = "grades" + '[' + (i + 1) + ']' + '.';
      r.put( Header.time.name(), map.get(prefixGrades + Header.time.field) );
      r.put( Header.grade.name(), map.get(prefixGrades + Header.grade.field) );
      r.put( Header.score.name(), map.get(prefixGrades + Header.score.field) );

      r.put( Header.name.name(), map.get(Header.name.field) );
      r.put( Header.restaurantId.name(), map.get(Header.restaurantId.field) );//*/
      
      final String prefixGrades = "grades" + '[' + (i + 1) + ']' + '.';
      for( int j=0; j<attributes.length; j++ ) {
        if( j == 7 || j == 8 || j == 9 ) { // grades fields
          r.put( RestaurantWritable.attributes[j], map.get( prefixGrades + RestaurantCSV.attributes[j] ) );
        } else {
          r.put( RestaurantWritable.attributes[j], map.get( RestaurantCSV.attributes[j] ) );
        }
      }
      flatGrades.add( r );
    }
    return flatGrades;
  }
}
