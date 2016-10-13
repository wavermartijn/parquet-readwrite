package com.meteogroup.domain;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

@Getter
@Setter
public class Model implements IndexedRecord{
  public String name;
  public String type;
  public float[] data1;
  public float[] data2;
  public float[] data3;
  public float[] data4;
  public float[] data5;
  public float[] data6;
  public float[] data7;
  public float[] data8;
  public float[] data9;
  public float[] data10;

  private static final int NAME_POSITION = 0;
  private static final int TYPE_POSITION = 1;
  private static final int DATA_POSITION = 2;

  @Override
  public void put(int i, Object o) {

  }

  @Override
  public Object get(int i) {
    if (i==NAME_POSITION){
      return name;
    }
    else if (i ==TYPE_POSITION){
      return type;
    }
    else if (i==DATA_POSITION){
      return data1;
    }
    else if (i==3){
      return data2;
    }
    else if (i==4){
      return data3;
    }
    else if (i==5){
      return data4;
    }
    else if (i==6){
      return data5;
    }
    else if (i==7){
      return data6;
    }
    else if (i==8){
      return data7;
    }
    else if (i==9){
      return data8;
    }
    else if (i==10){
      return data9;
    }
    else if (i==11){
      return data10;
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
