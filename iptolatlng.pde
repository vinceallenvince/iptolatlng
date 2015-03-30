import java.util.*;

String networkGeoIDCountryPath = "GeoLite2-Country-CSV/GeoLite2-Country-Blocks-IPv4.csv";
String networkGeoIDCountryCSV[];
String networkGeoIDCountryData[][];
int totalNetworkGeoIDCountries;
HashMap<String, StringList> hm_networkGeoIDCountry = new HashMap<String, StringList>();

String networkGeoIDCityPath = "GeoLite2-City-CSV/GeoLite2-City-Blocks-IPv4.csv";
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

void setup()
{
  start = new Date();
   println("Started loading data: " + start);
   createNetworkGeoIDCountryHM();
   /*createGeoIDDataCountryHM();
   createNetworkGeoIDCityHM();
   createGeoIDDataCityHM();*/
  loadSourceEndSong();
  end = new Date();
  println("Ended loadng data: " + end);
}

void draw()
{
  noLoop();
  // iterate over sourceEndSongs
  for (String s : sourceEndSongs.keys()) {
    // lookup the ip in createNetworkGeoIDCountryHM
  }
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
    String network = networkGeoIDCountryData[i][0];
    String geoname_id = networkGeoIDCountryData[i][1];

    // create loop to create range of network keys
    // find "/" and use digit before and after
    // use split()
    String[] listA = split(network, '/');
    int high = int(listA[listA.length - 1]);
    
    String[] listB = split(listA[0], '.');
    int low = int(listB[listB.length - 1]);
    
    println(low + ":" + high);

    StringList data = new StringList();
    data.append(network);
    data.append(geoname_id);

    hm_networkGeoIDCountry.put(network, data);
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
    String network = networkGeoIDCityData[i][0];
    String geoname_id = networkGeoIDCityData[i][1];

    StringList data = new StringList();
    data.append(network);
    data.append(geoname_id);

    hm_networkGeoIDCity.put(network, data);
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

