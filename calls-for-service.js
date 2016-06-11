/*
The purpose of this exercise is to populate MemSQL with a simple dataset that
includes both time and geographic attributes.  Specifcally, this module:
  (a) Grabs a file from the local filesystem
  (b) Formats the data
  (c) Loads it into MemSQL

The original data set came from https://dev.socrata.com/foundry/data.baltimorecity.gov/m8g9-abgb
*/
var csv = require("fast-csv");
var fs = require("fs");
var mysql = require("mysql");

var inputFilePath = "Calls_for_Service.csv";
var outputFilePath = "Calls_for_Service_formatted.csv";
var tableName = "calls_for_service";

var connection = mysql.createConnection({
  host     : "10.2.1.26",
  user     : "root",
  password : "",
  database : "baltimore"
});

var dataExists = true;

// check to see if the formatted data file exists
fs.access(outputFilePath, fs.F_OK, function(error) {
  if (error) {
    dataExists = false;
  }
});

// if the formatted file does not exist format the data
if (!dataExists) {

  var fileInputStream = fs.createReadStream(inputFilePath);
  var fileOutputStream = fs.createWriteStream(outputFilePath);

  csv.fromStream(fileInputStream, { headers:true })
    .on("data", function(data) {

      // format the date through string manipulation
      var chunks = data.callDateTime.split(" ");
      // date
      var dateChunks = chunks[0].split("/");
      var formattedDate = dateChunks[2]+'-'+dateChunks[0]+'-'+dateChunks[1];
      // time
      var timeChunks = chunks[1].split(":");
      var hours = timeChunks[0];
      AMPM = chunks[2];
      if ("AM" == AMPM && hours == 12) {
        hours = hours - 12;
      }
      if ("PM" == AMPM && hours < 12) {
        hours = hours + 12;
      }
      var formattedTime = hours + ":" + timeChunks[1] + ":" + timeChunks[2];
      // date and time
      var formattedDateTime = formattedDate + " " + formattedTime;
      // replace the unformated date and time with the formatted date and time
      data.callDateTime = formattedDateTime;

      // if there is a location, create latitude, longitude, and geo point in the json object
      var latitude = "";
      var longitude = "";
  //    var geo = "";
      if ("(,)" != data.location) {
        var tmpStr = data.location.replace(","," ");
        tmpStr = tmpStr.replace("(","");
        tmpStr = tmpStr.replace(")","");
        var latLong = tmpStr.split(" ");
        latitude = latLong[0];
        longitude = latLong[1];
        // create memSQL point syncax and set the geo attribute
  //      geo = "POINT" + data.location.replace(","," ") + "";
      }
      // remove the original location attribute
      delete data.location;
      // add the derived location attributes
      data.latitude = latitude;
      data.longitude = longitude;
  //    data.geo = geo;

      // write formatted CSV to file
      csv.writeToString(
      [data],
      function(err, data1){
          //console.log(data1);
          fileOutputStream.write(data1 + "\n");
      });
    })
    .on("end", function() {
      // when reaching EOF
    });

}

// load data into MemSQL
connection.connect();
var queryStr = "load data local infile " + "'" + outputFilePath + "'" +
  " into table " + tableName +
  " fields terminated by ','" +
  " optionally enclosed by '\"' " +
  " (call_date_time, priority, district, description, call_number, incident_location, latitude, longitude)"

console.log(queryStr);

var query = connection.query(queryStr, function(error, result) {
  if (error) {
    console.log("\n===================")
    console.log(error);
  }
});

connection.end();
