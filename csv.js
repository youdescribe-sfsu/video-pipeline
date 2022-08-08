const csv = require("csv-parser");
const fs = require("fs");
let temp = [];
let list = [];
let results = [];
let frameindex = [];
let timestamp = [];
var stringify = require("csv-stringify");
let iskeyFrame = [];
let description = [];
let columns = {
  frameindex: "frame",
  timestamp: "timestamp",
  Line1: "Line1",
  Line2: "Line2",
  Sim: "Similarity",
  Averageone: "avgone",
  Averagetwo: "avgtwo",
  iskeyFrame: "iskeyFrame",
  description: "description",
};
var similarity = require("compute-cosine-similarity");
fs.createReadStream("Captions and Objects.csv")
  .pipe(csv())
  .on("data", (data) => {
    results.push(data);
  })
  .on("end", () => {
    for (let i = 0; i < results.length; i++) {
      let keys = [];
      Object = results[i];
      for (let key in Object) {
        keys.push(key);
      }
      iskeyFrame.push(Object[keys[2]]);
      description.push(Object[keys[3]]);
      frameindex.push(parseFloat(Object[keys[0]]));
      timestamp.push(parseFloat(Object[keys[1]]));
      for (let i = 4; i < keys.length; i++) {
        if (Object[keys[i]] != "") {
          temp.push(parseFloat(Object[keys[i]]));
        } else {
          temp.push(0.0);
        }
      }
      list.push(temp);
      temp = [];
    }
    var data = [];
    for (var i = 2; i < list.length - 1; i += 1) {
      var s = similarity(list[i], list[i + 1]);
      if (i < list.length - 3) {
        var a1 = similarity(list[i - 1], list[i + 2]);
        var a2 = similarity(list[i - 2], list[i + 3]);
      } else {
        a1 = 0.0;
        a2 = 0.0;
      }
      if (isNaN(s)) {
        s = "SKIP";
      }
      data.push([
        frameindex[i],
        timestamp[i],
        i,
        i + 1,
        s,
        a1,
        a2,
        iskeyFrame[i],
        description[i],
      ]);
    }
    stringify(data, { header: true, columns: columns }, (err, output) => {
      if (err) throw err;
      fs.writeFile("outputavg.csv", output, (err) => {
        if (err) throw err;
        console.log("outputavg.csv saved.");
      });
    });
  });
