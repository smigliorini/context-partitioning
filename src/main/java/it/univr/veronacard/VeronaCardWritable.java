package it.univr.veronacard;

import it.univr.hadoop.ContextData;
import it.univr.veronacard.partitioning.DataUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class VeronaCardWritable
  extends VeronaCardRecord
  implements ContextData {

  /**
   * Default partitioning fields
   */
  public static final Integer[] DEFAULT_PARTITION = { 1,2,3,5 }; // x, y, time, age

  /**
   * Fields of the CSV file
   */
  public static final String[] attributes = {
          //"x", "y", "time", "age"
          "vcSerial",
          "x",
          "y",
          "time",
          "poiName",
          "age"
  };

  //static final Logger LOGGER = LogManager.getLogger( VeronaCardWritable.class );

  public static String SPLITERATOR = ",";

  public VeronaCardWritable() {
    super();
  }

  public VeronaCardWritable( VeronaCardRecord veronaCardRecord ) {
    super( veronaCardRecord );
  }

  public VeronaCardWritable( VeronaCardWritable veronaCardWritable ) {
    super( veronaCardWritable );
  }

  @Override
  public String[] getContextFields( Integer[] partition ) {
    // Default value
    if( partition == null || partition.length < 2 ) {
      //LOGGER.warn( format( "Number of fields for partitioner is < %d.%d", 2, partition.length ) );
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
      //LOGGER.warn( format( "Field index %d is not exist.", position ) );
      return false;
    }
    return true;
  }

  @Override
  public int compareTo( ContextData o ) {
    //TODO Do we need a sort algorithm?
    VeronaCardRecord veronaCardRecord = (VeronaCardRecord) o;
    if( this.time < veronaCardRecord.time )
      return -1;
    if( this.time > veronaCardRecord.time )
      return 1;
    if( this.x < veronaCardRecord.x )
      return 1;
    if( this.x > veronaCardRecord.x )
      return -1;
    return 0;
  }

  @Override
  public void write( DataOutput out ) throws IOException {
    //new Text(vcSerial).write(out);
    try {
      out.writeUTF( vcSerial.toString() );
    } catch( NullPointerException e ) {
      out.writeUTF( "null" );
    }
    out.writeDouble( x );
    out.writeDouble( y );
    //out.writeLong(time);
    out.writeDouble( time );
    //new Text(poiName).write(out);
    try {
      out.writeUTF( poiName.toString() );
    } catch( NullPointerException e ) {
      out.writeUTF( "null" );
    }
    //out.writeInt(age);
    out.writeDouble( age );
  }

  @Override
  public void readFields( DataInput in ) throws IOException {
    //Text serial = new Text();
    //serial.readFields(in);
    //vcSerial = serial.toString();
    vcSerial = in.readUTF();
    x = in.readDouble();
    y = in.readDouble();
    //time = in.readLong();
    time = in.readDouble();
    //Text poi = new Text();
    //poi.readFields(in);
    //poiName = poi.toString();
    poiName = in.readUTF();
    //age = in.readInt();
    age = in.readDouble();
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

  public VeronaCardWritable parseRecord( String line ) {
    VeronaCardRecord veronaCardRecord = DataUtils.parseRecord( line, SPLITERATOR );
    return new VeronaCardWritable( veronaCardRecord );
  }
  
  @Override
  public String toString() { return toString( SPLITERATOR ); }
  
  public String toString( String separator ) {
    final StringBuilder b = new StringBuilder();

    b.append( x );
    b.append( separator );
    b.append( y );
    b.append( separator );
    b.append( time );
    b.append( separator );
    b.append( age );
    b.append( separator );
    b.append( x );
    b.append( separator );
    b.append( y );
    b.append( separator );
    b.append( time );
    b.append( separator );
    b.append( age );

    return b.toString();
  }
}
