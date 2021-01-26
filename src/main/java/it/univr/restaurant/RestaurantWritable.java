package it.univr.restaurant;

import it.univr.hadoop.ContextData;
import it.univr.restaurant.partitioning.DataUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class RestaurantWritable
  extends RestaurantRecord
  implements ContextData {

  /**
   * Default partitioning fields
   */
  // todo: 6 partizioni
  /*public static final Integer[] DEFAULT_PARTITION = { 1,2,4,7,9,11 }; // coordX, coordY, zipcode, $date, score, restaurantId //*/
  public static final Integer[] DEFAULT_PARTITION = { 1,2,4,7,9 }; // coordX, coordY, zipcode, $date, score //*/

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
          "time",
          "grade",
          "score",
          "name",
          "restaurantId"
  };

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
  public String[] getContextFields( Integer[] partition ) {
    // Default value
    if( partition == null || partition.length < 2 ) {
      return getAttributes( DEFAULT_PARTITION );
    }
    return getAttributes( partition );
  }

  public String[] getAttributes( Integer[] partition ) {
    final Set<String> fields = new HashSet<>();

    for( Integer n : partition ) {
      if( checkAttribute( n ) )
        fields.add( attributes[n] );
    }
    return fields.toArray( new String[fields.size()] );
  }

  private boolean checkAttribute( Integer position ) {
    // invalid attribute
    if( position == null ) {
      return false;
    }
    // out of range
    if( position < 0 || position >= attributes.length ) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo( ContextData o ) {
    RestaurantRecord restaurantRecord = ( RestaurantRecord ) o;
    if ( this.time < restaurantRecord.time)
      return -1;
    if ( this.time > restaurantRecord.time)
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
      out.writeUTF( building.toString() );
    } catch( NullPointerException e ) {
      // building can be empty string
      out.writeUTF( "null" );
    }
    out.writeDouble( coordX );
    out.writeDouble( coordY );
    out.writeUTF( street.toString() );
    out.writeDouble( zipcode );
    out.writeUTF( borough.toString() );
    out.writeUTF( cuisine.toString() );
    out.writeDouble( time );
    out.writeUTF( grade.toString() );
    out.writeDouble( score );
    out.writeUTF( name.toString() );
    out.writeDouble( restaurantId );
  }

  @Override
  public void readFields( DataInput in ) throws IOException {
    building = in.readUTF();
    coordX = in.readDouble();
    coordY = in.readDouble();
    street = in.readUTF();
    zipcode = in.readDouble();
    borough = in.readUTF();
    cuisine = in.readUTF();
    time = in.readDouble();
    grade = in.readUTF();
    score = in.readDouble();
    name = in.readUTF();
    restaurantId = in.readDouble();
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
    b.append( time );
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
