import java.util.*;

/**
 * Need to convert geoip Block files via geoip2-csv-converter (https://github.com/maxmind/geoip2-csv-converter).
 * You should include the integer range via -include-integer-range=true. A sample command looks like:
 * ./geoip2-csv-converter -block-file=GeoLite2-Country-Blocks-IPv4.csv -include-integer-range=true -output-file=output.csv
 * 
 */

String networkGeoIDCountryPath = "GeoLite2-Country-CSV/GeoLite2-Country-Blocks-IPv4-extended.csv";
String networkGeoIDCountryCSV[];
String networkGeoIDCountryData[][];
int totalNetworkGeoIDCountries;
HashMap<String, StringList> hm_networkGeoIDCountry = new HashMap<String, StringList>();

String networkGeoIDCityPath = "GeoLite2-City-CSV/GeoLite2-City-Blocks-IPv4-extended.csv";
String networkGeoIDCityCSV[];
String networkGeoIDCityData[][];
int totalNetworkGeoIDCities;
HashMap<String, StringList> hm_networkGeoIDCity = new HashMap<String, StringList>();

String geoIDDataCountryPath = "GeoLite2-Country-CSV/GeoLite2-Country-Locations-en.csv";
String geoIDDataCountryCSV[];
String geoIDDataCountryData[][];
int totalGeoIDDataCountries;
HashMap<String, StringList> hm_geoIDDataCountry = new HashMap<String, StringList>();

String geoIDDataCityPath = "GeoLite2-City-CSV/GeoLite2-City-Locations-en.csv";
String geoIDDataCityCSV[];
String geoIDDataCityData[][];
int totalGeoIDDataCities;
HashMap<String, StringList> hm_geoIDDataCity = new HashMap<String, StringList>();

String sourceEndSongPath = "source.csv";
String sourceEndSongCSV[];
String sourceEndSongData[][];
int totalSourceEndSongRows;
StringDict sourceEndSongs = new StringDict();
Date start;
Date end;

String outputSavePath = "output/output.csv";
IntDict countryStreams = new IntDict();
int counter = 0;

void setup()
{
  start = new Date();
  println("Started loading data: " + start);
  loadSourceEndSong();

  createNetworkGeoIDCountryHM();
  createGeoIDDataCountryHM();

  //createNetworkGeoIDCityHM();
  //createGeoIDDataCityHM();

  end = new Date();
  println("Ended loadng data: " + end);
}

void draw()
{
  noLoop();
  // iterate over sourceEndSongs
  for (String ip : sourceEndSongs.keys ()) {

    //if (counter > 200) break;

    String octets[] = split(ip, ".");
    int octet1 = int(octets[0]);
    int octet2 = int(octets[1]);
    int octet3 = int(octets[2]);
    int octet4 = int(octets[3]);
    int networkInteger = ipToInterger(octet1, octet2, octet3, octet4);

    println("Source integer: " + networkInteger);

    // lookup the ip in createNetworkGeoIDCountryHM 
    for (Map.Entry row : hm_networkGeoIDCountry.entrySet ()) {
      StringList net = (StringList)row.getValue();
      int network_start = int(net.get(0));
      int network_end = int(net.get(1));
      String geoid = net.get(2);
      if (networkInteger >= network_start && networkInteger <= network_end) {
        println();
        println(ip);
        println("networkInteger: " + networkInteger + " Range: " + network_start + " -> " + network_end);
        println("geoid: " + geoid);

        StringList geoIDData = hm_geoIDDataCountry.get(geoid);  
        println(geoIDData.get(4));      
        
        // 
        int prevStreams = countryStreams.get(geoIDData.get(4)); // will return 0 if key does not exist
        int combinedStreams = int(sourceEndSongs.get(ip)) + prevStreams;
        
        countryStreams.set(geoIDData.get(4), combinedStreams);
        break;
      }
    }
    counter++;
  }

  saveOutput();
}

void loadSourceEndSong()
{
  sourceEndSongCSV = loadStrings(sourceEndSongPath);
  totalSourceEndSongRows = sourceEndSongCSV.length;
  println("Total sourceEndSongRows loaded: " + totalSourceEndSongRows);
  sourceEndSongData = new String[totalSourceEndSongRows][2];
  for (int i = 0; i < totalSourceEndSongRows; i++) {
    sourceEndSongData[i] = sourceEndSongCSV[i].split(",");
  }
  println("Total sourceEndSongRows sorted: " + sourceEndSongData.length);
  for (int i = 0; i < sourceEndSongData.length; i++) {
    String ip = sourceEndSongData[i][0];
    String streams = sourceEndSongData[i][1];
    sourceEndSongs.set(ip, streams);
  }
  sourceEndSongCSV = null;
  sourceEndSongData = null;
  println("Total sourceEndSongRows processed: " + sourceEndSongs.size());
}

void saveOutput()
{
  PrintWriter output;
  output = createWriter(outputSavePath);
  for (String s : countryStreams.keys ()) {
    output.println(s + "," + countryStreams.get(s));
  }
  // save the file
  output.close();
}

// returns (first octet * 256³) + (second octet * 256²) + (third octet * 256) + (fourth octet)
int ipToInterger(int octet1, int octet2, int octet3, int octet4)
{
  return (octet1 * 16777216) + (octet2 * 65536) + (octet3 * 256) + (octet4);
}

// !! should we use an Array here since we have to iterate thru it to find what we want
void createNetworkGeoIDCountryHM()
{
  networkGeoIDCountryCSV = loadStrings(networkGeoIDCountryPath);
  totalNetworkGeoIDCountries = networkGeoIDCountryCSV.length;
  println("Total networkGeoIDCountries loaded: " + totalNetworkGeoIDCountries);
  networkGeoIDCountryData = new String[totalNetworkGeoIDCountries][2];
  for (int i = 0; i < totalNetworkGeoIDCountries; i++) {
    networkGeoIDCountryData[i] = networkGeoIDCountryCSV[i].split(",");
  }
  println("Total networkGeoIDCountries sorted: " + networkGeoIDCountryData.length);
  for (int i = 0; i < networkGeoIDCountryData.length; i++) {
    String network_start = networkGeoIDCountryData[i][0];
    String network_end = networkGeoIDCountryData[i][1];
    String geoname_id = networkGeoIDCountryData[i][2];

    StringList data = new StringList();
    data.append(network_start);
    data.append(network_end);
    data.append(geoname_id);

    hm_networkGeoIDCountry.put(network_start + network_end, data);
  }
  networkGeoIDCountryCSV = null;
  networkGeoIDCountryData = null;
  println("Total networkGeoIDCountries processed: " + hm_networkGeoIDCountry.size());
}

void createGeoIDDataCountryHM()
{
  geoIDDataCountryCSV = loadStrings(geoIDDataCountryPath);
  totalGeoIDDataCountries = geoIDDataCountryCSV.length;
  println("Total geoIDDataCountries loaded: " + totalGeoIDDataCountries);
  geoIDDataCountryData = new String[totalGeoIDDataCountries][5];
  for (int i = 0; i < totalGeoIDDataCountries; i++) {
    geoIDDataCountryData[i] = geoIDDataCountryCSV[i].split(",");
  }
  println("Total geoIDDataCountries sorted: " + geoIDDataCountryData.length);
  for (int i = 0; i < geoIDDataCountryData.length; i++) {
    String geoname_id = geoIDDataCountryData[i][0];
    String locale_code = geoIDDataCountryData[i][1];
    String continent_code = geoIDDataCountryData[i][2];
    String continent_name = geoIDDataCountryData[i][3];
    String country_iso_code = geoIDDataCountryData[i][4];
    String country_name = geoIDDataCountryData[i][5];

    StringList data = new StringList();
    data.append(geoname_id);
    data.append(locale_code);
    data.append(continent_code);
    data.append(continent_name);
    data.append(country_iso_code);
    data.append(country_name);

    hm_geoIDDataCountry.put(geoname_id, data);
  }
  geoIDDataCountryCSV = null;
  geoIDDataCountryData = null;
  println("Total geoIDDataCountries processed: " + hm_geoIDDataCountry.size());
}

void createNetworkGeoIDCityHM()
{
  networkGeoIDCityCSV = loadStrings(networkGeoIDCityPath);
  totalNetworkGeoIDCities = networkGeoIDCityCSV.length;
  println("Total networkGeoIDCities loaded: " + totalNetworkGeoIDCities);
  networkGeoIDCityData = new String[totalNetworkGeoIDCities][2];
  for (int i = 0; i < totalNetworkGeoIDCities; i++) {
    networkGeoIDCityData[i] = networkGeoIDCityCSV[i].split(",");
  }
  println("Total networkGeoIDCities sorted: " + networkGeoIDCityData.length);
  for (int i = 0; i < networkGeoIDCityData.length; i++) {
    String network_start = networkGeoIDCountryData[i][0];
    String network_end = networkGeoIDCountryData[i][1];
    String geoname_id = networkGeoIDCountryData[i][2];

    StringList data = new StringList();
    data.append(network_start);
    data.append(network_end);
    data.append(geoname_id);

    hm_networkGeoIDCity.put(network_start + network_end, data);
  }
  networkGeoIDCityCSV = null;
  networkGeoIDCityData = null;
  println("Total networkGeoIDCities processed: " + hm_networkGeoIDCity.size());
}

void createGeoIDDataCityHM()
{
  geoIDDataCityCSV = loadStrings(geoIDDataCityPath);
  totalGeoIDDataCities = geoIDDataCityCSV.length;
  println("Total geoIDDataCities loaded: " + totalGeoIDDataCities);
  geoIDDataCityData = new String[totalGeoIDDataCities][5];
  for (int i = 0; i < totalGeoIDDataCities; i++) {
    geoIDDataCityData[i] = geoIDDataCityCSV[i].split(",");
  }
  println("Total geoIDDataCities sorted: " + geoIDDataCityData.length);
  for (int i = 0; i < geoIDDataCityData.length; i++) {
    String geoname_id = geoIDDataCityData[i][0];
    String locale_code = geoIDDataCityData[i][1];
    String continent_code = geoIDDataCityData[i][2];
    String continent_name = geoIDDataCityData[i][3];
    String country_iso_code = geoIDDataCityData[i][4];
    String country_name = geoIDDataCityData[i][5];

    StringList data = new StringList();
    data.append(geoname_id);
    data.append(locale_code);
    data.append(continent_code);
    data.append(continent_name);
    data.append(country_iso_code);
    data.append(country_name);

    hm_geoIDDataCity.put(geoname_id, data);
  }
  geoIDDataCityCSV = null;
  geoIDDataCityData = null;
  println("Total geoIDDataCities processed: " + hm_geoIDDataCity.size());
}

