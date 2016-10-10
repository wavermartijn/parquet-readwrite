package com.meteogroup;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class DemoController {

  @Autowired
  ParquetWriteService parquetWriteService;

  @RequestMapping(name = "/martijn", method = RequestMethod.GET)
  public String demoEndpoint() throws IOException {
    parquetWriteService.writeExampleParquetFile();
    return "ok, it works";
  }
}
