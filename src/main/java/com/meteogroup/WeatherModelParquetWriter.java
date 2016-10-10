package com.meteogroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

public class WeatherModelParquetWriter extends ParquetWriter.Builder {


    public WeatherModelParquetWriter(Path file) {
    super(file);
  }

  public ParquetWriter.Builder self() {
    return this;
  }

  protected WriteSupport getWriteSupport(Configuration configuration) {

    WriteSupport avroWriteSupport = new AvroWriteSupport();


    return avroWriteSupport;
  }
}
