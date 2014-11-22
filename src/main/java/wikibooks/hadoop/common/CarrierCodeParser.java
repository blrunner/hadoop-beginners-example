package wikibooks.hadoop.common;

import org.apache.hadoop.io.Text;

public class CarrierCodeParser {
  private String carrierCode;
  private String carrierName;

  public CarrierCodeParser(Text value) {
    this(value.toString());
  }

  public CarrierCodeParser(String value) {
    try {
      String[] colums = value.split(",");
      if (colums != null && colums.length > 0) {
        carrierCode = colums[0];
        carrierName = colums[1];
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }

  public String getCarrierCode() {
    return carrierCode;
  }

  public String getCarrierName() {
    return carrierName;
  }
}
