package com.meteogroup.service;

import com.meteogroup.domain.Model;
import com.meteogroup.WeatherModelParquetWriterBuilder;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;


  @Service
  public class ParquetWriteService {

    private static final String AVRO_SCHEMA = "parquet.avro.schema";
    private static final Schema MAP_KEY_SCHEMA = Schema.create(Schema.Type.STRING);

    Configuration configuration = null;

    @PostConstruct
    public void initConfiguration() throws IOException {
      configuration = retrieveConfiguration();
    }


    public void writeExampleParquetFile(String compressionType) throws IOException {
      ParquetWriter parquetWriter = initWriter(compressionType, new Path("/user/maria_dev/martijn_" + System.currentTimeMillis()));
      Model model = new Model();
      model.name = "martijn";
      model.type="testtype";

      for (int teller=0; teller<10; teller++ ){
        float[] data = new float[300_000];
        for(int i=0; i< data.length; i++){
          data[i] = (float) (250 + Math.random() * 10);
        }
        if (teller==0){
          model.data1=data;
        }
        else if (teller==1){
          model.data2=data;
        }
        else if (teller==2){
          model.data3=data;
        }
        else if (teller==3){
          model.data4=data;
        }
        else if (teller==4){
          model.data5=data;
        }
        else if (teller==5){
          model.data6=data;
        }
        else if (teller==6){
          model.data7=data;
        }
        else if (teller==7){
          model.data8=data;
        }
        else if (teller==8){
          model.data9=data;
        }
        else if (teller==9){
          model.data10=data;
        }
      }


      parquetWriter.write(model);
      parquetWriter.close();
    }

    public void writeExampleTextFile() throws IOException {
          FileSystem fs = FileSystem.get(configuration);
          FSDataOutputStream fdos=fs.create(new Path("/user/maria_dev/file01" + System.currentTimeMillis() + ".txt"), true);
          fdos.writeBytes("Test text for the txt file");
          fdos.flush();
          fdos.close();
          fs.close();
    }



    private ParquetWriter initWriter(String compressionType,Path filePath) throws IOException {

      WeatherModelParquetWriterBuilder builder = new WeatherModelParquetWriterBuilder(filePath);

      CompressionCodecName codecName = null;
      if (compressionType.equalsIgnoreCase("snappy")){
        codecName = CompressionCodecName.SNAPPY;
      }
      else if (compressionType.equalsIgnoreCase("gzip")){
        codecName = CompressionCodecName.GZIP;
      }
      else if (compressionType.equalsIgnoreCase("lzo")){
        codecName = CompressionCodecName.LZO;

      }

      return builder.withCompressionCodec(codecName).withPageSize(3600)
          .withWriteMode(ParquetFileWriter.Mode.CREATE)
          .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
          .withConf(configuration)

          .enableValidation().build();

    }

    private Configuration retrieveConfiguration() throws IOException {
      System.setProperty("HADOOP_USER_NAME", "hdfs");
      configuration = new Configuration();
      configuration.set(AVRO_SCHEMA, IOUtils.toString(ParquetWriteService.class.getResourceAsStream("/model.avsc")));
      configuration.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");
      configuration.set("fs.default.name", "hdfs://sandbox.hortonworks.com:8020");
      configuration.set("hadoop.job.ugi", "hbase");
      configuration.set("dfs.datanode.address", "sandbox.hortonworks.com:50075");
      configuration.set("dfs.replication","1");
      return configuration;
    }
  }

