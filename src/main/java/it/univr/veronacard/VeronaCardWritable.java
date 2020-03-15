package it.univr.veronacard;

import it.univr.hadoop.ContextData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VeronaCardWritable extends VeronaCardRecord implements ContextData {


    public VeronaCardWritable (){
        super();
    }

    public VeronaCardWritable (VeronaCardRecord veronaCardRecord){
        super(veronaCardRecord);
    }

    public VeronaCardWritable(VeronaCardWritable veronaCardWritable) {
        super(veronaCardWritable);
    }

    @Override
    public String[] getContextFields() {
        return new String[]{"time", "x", "y", "age"};
    }

    @Override
    public int compareTo(ContextData o) {
        //draft sort
        VeronaCardRecord veronaCardRecord = (VeronaCardRecord) o;
        if(this.time < veronaCardRecord.time)
            return -1;
        if(this.time > veronaCardRecord.time)
            return 1;

        if(this.x < veronaCardRecord.x)
            return 1;
        if(this.x > veronaCardRecord.x)
            return -1;
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(vcSerial);
        out.writeDouble(x);
        out.writeDouble(y);
        out.writeLong(time);
        out.writeUTF(poiName);
        out.writeInt(age);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vcSerial = in.readUTF();
        x = in.readDouble();
        y = in.readDouble();
        time = in.readLong();
        poiName = in.readUTF();
        age = in.readInt();
    }

    @Override
    protected VeronaCardWritable clone(){
        return new VeronaCardWritable(this);
    }
}
