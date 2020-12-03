package it.univr.restaurant;

import it.univr.hadoop.ContextData;
import it.univr.restaurant.partitioning.DataUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class RestaurantWritable
  extends RestaurantRecord
  implements ContextData {

  /**
   * List of partitioner
   */
  private static final int[] partitioner = { 1,2,7,9 }; // all partition //*/
  /*private static final int[] partitioner = { 1,2 }; // coordX, coordY //*/

  /**
   * Fields of the input file
   */
  public static final String[] attributes = {
          "building",
          "coordX",
          "coordY",
          "street",
          "zipcode",
          "borough",
          "cuisine",
          "$date",
          "grade",
          "score",
          "name",
          "restaurantId"
  };

  static final Logger LOGGER = LogManager.getLogger( RestaurantWritable.class );

  public static String SPLITERATOR = ",";

  public RestaurantWritable() {
    super();
  }

  public RestaurantWritable( RestaurantRecord restaurantRecord ) {
    super( restaurantRecord );
  }

  public RestaurantWritable( RestaurantWritable restaurantWritable ) {
    super( restaurantWritable );
  }

  @Override
  public String[] getContextFields() {
    //return new String[]{"coordX", "coordY", "$date", "score"};
    return getContextFields( partitioner );
  }

  public String[] getContextFields( int[] indexFields ) {
    List<String> partFields = new ArrayList<>();

    for( int indexField : indexFields ) {
      if( indexField >= 0 && indexField < attributes.length  )
        partFields.add( attributes[indexField] );
      else
        LOGGER.warn( format( "Field index %d is not exist.", indexField ) );
    }

    if( partFields.size() == 0 ) {
      // TODO
      LOGGER.error( format( "Context fields array is empty, choose at least two: %s" ) );
      System.exit(-1);
    }
    return partFields.toArray( new String[partFields.size()] );
  }

  @Override
  public int compareTo( ContextData o ) {
    RestaurantRecord restaurantRecord = ( RestaurantRecord ) o;
    if ( this.$date < restaurantRecord.$date )
      return -1;
    if ( this.$date > restaurantRecord.$date )
      return 1;
    if ( this.coordX < restaurantRecord.coordX )
      return 1;
    if ( this.coordX > restaurantRecord.coordX )
      return -1;
    if ( this.coordY < restaurantRecord.coordY )
     return 1;
    if ( this.coordY > restaurantRecord.coordY )
     return -1;
    return 0;
  }

  @Override
  public void write( DataOutput out ) throws IOException {
    try {
      out.writeUTF(building.toString());
    } catch (NullPointerException e) {
      // building can be empty string
      out.writeUTF("null");
    }
    out.writeDouble(coordX);
    out.writeDouble(coordY);
    out.writeUTF(street.toString());
    out.writeInt(zipcode);
    out.writeUTF(borough.toString());
    out.writeUTF(cuisine.toString());
    out.writeDouble($date);
    out.writeUTF(grade.toString());
    out.writeDouble(score);
    out.writeUTF(name.toString());
    out.writeInt(restaurantId);
  }

  @Override
  public void readFields( DataInput in ) throws IOException {
    building = in.readUTF();
    coordX = in.readDouble();
    coordY = in.readDouble();
    street = in.readUTF();
    zipcode = in.readInt();
    borough = in.readUTF();
    cuisine = in.readUTF();
    $date = in.readDouble();
    grade = in.readUTF();
    score = in.readDouble();
    name = in.readUTF();
    restaurantId = in.readInt();
  }

  @Override
  public boolean equals( Object o ) {
    //TODO related to hashcode
    return super.equals( o );
  }

  @Override
  public int hashCode() {
    //TODO Read documentation Hadoop about hash calculation
    return super.hashCode();
  }

  public RestaurantWritable parseRecord( String line ) {
    RestaurantRecord restaurantRecord = DataUtils.parseRecord( line, SPLITERATOR );
    return new RestaurantWritable( restaurantRecord );
  }

  @Override
  public String toString() { return toString( SPLITERATOR ); }

  public String toString( String separator ) {
    final StringBuilder b = new StringBuilder();

    b.append( building );
    b.append( separator );
    b.append( coordX );
    b.append( separator );
    b.append( coordY );
    b.append( separator );
    b.append( street );
    b.append( separator );
    b.append( zipcode );
    b.append( separator );
    b.append( borough );
    b.append( separator );
    b.append( cuisine );
    b.append( separator );
    b.append( $date );
    b.append( separator );
    b.append( grade );
    b.append( separator );
    b.append( score );
    b.append( separator );
    b.append( name );
    b.append( separator );
    b.append( restaurantId );

    return b.toString();
  }
}
