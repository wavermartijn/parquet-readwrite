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

  @Override
  public void put(int i, Object o) {

  }

  @Override
  public Object get(int i) {
    if (i==0){
      return name;
    }
    else if (i ==1){
      return type;
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
