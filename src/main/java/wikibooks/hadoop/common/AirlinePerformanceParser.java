package wikibooks.hadoop.common;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {

  private int year;
  private int month;

  private int arriveDelayTime = 0;
  private int departureDelayTime = 0;
  private int distance = 0;

  private boolean arriveDelayAvailable = true;
  private boolean departureDelayAvailable = true;
  private boolean distanceAvailable = true;

  private String uniqueCarrier;

  public AirlinePerformanceParser(Text text) {
    try {
      String[] colums = text.toString().split(",");

      // 운항 연도 설정
      year = Integer.parseInt(colums[0]);

      // 운항 월 설정
      month = Integer.parseInt(colums[1]);

      // 항공사 코드 설정
      uniqueCarrier = colums[8];

      // 항공기 출발 지연 시간 설정
      if (!colums[15].equals("NA")) {
        departureDelayTime = Integer.parseInt(colums[15]);
      } else {
        departureDelayAvailable = false;
      }

      // 항공기 도착 지연 시간 설정
      if (!colums[14].equals("NA")) {
        arriveDelayTime = Integer.parseInt(colums[14]);
      } else {
        arriveDelayAvailable = false;
      }

      // 운항 거리 설정
      if (!colums[18].equals("NA")) {
        distance = Integer.parseInt(colums[18]);
      } else {
        distanceAvailable = false;
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }

  public int getYear() { return year; }

  public int getMonth() { return month; }

  public int getArriveDelayTime() { return arriveDelayTime; }

  public int getDepartureDelayTime() { return departureDelayTime; }

  public boolean isArriveDelayAvailable() { return arriveDelayAvailable; }

  public boolean isDepartureDelayAvailable() { return departureDelayAvailable;  }

  public String getUniqueCarrier() { return uniqueCarrier; }

  public int getDistance() { return distance; }

  public boolean isDistanceAvailable() { return distanceAvailable;  }
}
