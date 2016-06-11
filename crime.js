/*
The purpose of this exercise is to populate a database with a simple dataset
that includes both time and geographic attributes.  Specifcally, this module:
  (a) Downloads Victim Based Crime Data from Open Baltimore
  (b) Formats the data
  (c) Loads it into an RDBMS (in this case MemSQL)

  NOTE: This is a very inefficent script.  It does _not_ perform bulk loading.
  Instead, each line is inserted into the database one at a time.
*/
var csv = require("fast-csv");
var fs = require("fs");
var mysql = require("mysql");
var request = require("request");

var dataDownloadURL = "https://data.baltimorecity.gov/api/views/wsfq-mvij/rows.csv"
var filePath = "tmp-data/BPD_Part_1_Victim_Based_Crime_Data.csv";
//var filePath = "BPD_Part_1_Victim_Based_Crime_Data_small_set.csv";

var connection = mysql.createConnection({
  host     : "10.2.1.26",
  user     : "root",
  password : "",
  database : "baltimore"
});

// check to see if the local data file exists
fs.access(filePath, fs.F_OK, function(error) {

  // if the file does not exist
  if (error) {
    // download the file
    console.log("Downloading file...");
    request(dataDownloadURL, function (error, response) {
      if (error || response.statusCode != 200) {
        console.log("Download failed.");
        process.exit(1);
      }
    }).pipe(fs.createWriteStream(filePath));
  }

  insertData();

});

function insertData() {

  var fileInputStream = fs.createReadStream(filePath);

  // parse each line of the csv
  csv.fromStream(fileInputStream, { headers:true })
    .on("data", function(data) {

      // format the date through string manipulation
      var chunks = data.CrimeDate.split("/");
      var formattedDate = chunks[2]+'-'+chunks[0]+'-'+chunks[1];
      var dateTime = formattedDate + " " + data.CrimeTime;

      // create a json for inserting in the database
      var xform = {
        "crime_date_time" : dateTime,
        "crime_code"      : data.CrimeCode,
        "street"          : data.Location,
        "description"     : data.Description,
        "weapon"          : data.Weapon,
        "post"            : data.Post,
        "district"        : data.District,
        "neighborhood"    : data.Neighborhood,
        "num_incidents"   : data["Total Incidents"]
      };

      // add lat/long and point only if available
      if ("" != data["Location 1"]) {
        // derive and set the lat and long attributes
        var tmpStr = data["Location 1"].replace(",","");
        tmpStr = tmpStr.replace("(","");
        tmpStr = tmpStr.replace(")","");
        var latLong = tmpStr.split(" ");
        xform.latitude = latLong[0];
        xform.longitude = latLong[1];

        // create memSQL point syncax and set the geo attribute
        var geo = "POINT" + data["Location 1"].replace(",","") + "";
        xform.geo = geo;
      }

      // insert an individual record
      var query = connection.query('INSERT INTO victim_based_crime SET ?', xform, function(error, result) {
        if (error) {
          console.log("\n===================")
          console.log(error);
          console.log(xform);
        }
      });

    })
    .on("end", function() {
      // when reaching EOF, close the connection
      connection.end();
    });

}
