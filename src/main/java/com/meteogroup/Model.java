package com.meteogroup;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

@Getter
@Setter
public class Model implements IndexedRecord{
  public String name;
  public String type;
  public float[] data;

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
      return data;
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
