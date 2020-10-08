package it.univr.restaurant;

import it.univr.hadoop.ContextData;
import it.univr.restaurant.partitioning.DataUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RestaurantWritable
  extends RestaurantRecord
  implements ContextData {

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

  // TODO
  @Override
  public String[] getContextFields() {
    return new String[]{"coordX", "coordY", "$date", "grade", "score"};
  }

  @Override
  public int compareTo( ContextData o ) {
    // TODO
    RestaurantRecord restaurantRecord = (RestaurantRecord) o;
    if( this.$date < restaurantRecord.$date )
      return -1;
    if( this.$date > restaurantRecord.$date )
      return 1;
    return 0;
  }

  @Override
  public void write( DataOutput out ) throws IOException {
    //new Text(vcSerial).write(out);
    out.writeDouble( coordX );
    out.writeDouble( coordY );
    out.writeLong( $date );
    out.writeBytes( grade );
    out.writeInt( score );
  }

  @Override
  public void readFields( DataInput in ) throws IOException {
    //building = in.readLine();
    coordX = in.readDouble();
    coordY = in.readDouble();
    //street = in.readLine();
    //zipcode = in.readLine();
    $date = in.readLong();
    grade = in.readLine();
    score = in.readInt();
    //name = in.readLine();
    //restaurantId = in.readLine();
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
    return new RestaurantWritable(restaurantRecord);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append( building );
    b.append( "," );
    b.append( coordX );
    b.append( "," );
    b.append( coordY );
    b.append( "," );
    b.append( street );
    b.append( "," );
    b.append( zipcode );
    b.append( "," );
    b.append( borough );
    b.append( "," );
    b.append( cuisine );
    b.append( "," );
    b.append( $date );
    b.append( "," );
    b.append( grade );
    b.append( "," );
    b.append( score );
    b.append( "," );
    b.append( name );
    b.append( "," );
    b.append( restaurantId );
    return b.toString();
  }
}
