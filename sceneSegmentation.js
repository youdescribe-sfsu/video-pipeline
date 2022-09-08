const csv = require("csv-parser");
const fs = require("fs");
let temp = [];
let list = [];
let results = [];
let columns = {
  start_time: "start_time",
  end_time: "end_time",
  description: "description",
};
data = [];
var stringify = require("csv-stringify");
const scenesegmentedfile = process.argv[3];
const outputavg_csv = process.argv[2]
fs.createReadStream(outputavg_csv)
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

      for (let i = 0; i < keys.length; i++) {
        if (Object[keys[i]] != "") {
          if (i == 4) {
            if (Object[keys[i]] == "SKIP") {
              temp.push(Object[keys[i]]);
            } else {
              temp.push(parseFloat(Object[keys[i]]));
            }
          } else if (i == 7 || i == 8) {
            temp.push(Object[keys[i]]);
          } else {
            temp.push(parseFloat(Object[keys[i]]));
          }
        } else {
          temp.push(0.0);
        }
      }
      list.push(temp);
      temp = [];
    }
    function averagecheck(averageone, averagetwo, threshold) {
      if (averageone < threshold && averagetwo < threshold) {
        return true;
      } else {
        return false;
      }
    }
    //scene segmentation Logic
    //if there is skip for more 3 seconds then it has the possibility for new scene
    const SceneSegmentation = (sceneTimeLimit, threshold) => {
      let scenesegments = [];
      let currentSceneTimeStamp = 0;
      let firstSkip = false;
      let skiptimestamp;
      let description = "";
      for (let i = 0; i < list.length; i++) {
        if (list[i][7] == "True") {
          description = description + "\n" + list[i][8];
        }
        //if there is continous skip
        if (list[i][4] != "SKIP" && list[i][4] < threshold) {
          if (
            averagecheck(list[i][5], list[i][6], threshold) &&
            list[i][1] - currentSceneTimeStamp > sceneTimeLimit
          ) {
            scenesegments.push(list[i][1]);
            data.push([currentSceneTimeStamp, list[i][1], description]);
            description = "";
            currentSceneTimeStamp = list[i][1];
          }
        }
        if (list[i][4] != "SKIP" && firstSkip == true) {
          if (list[i][1] - skiptimestamp >= sceneTimeLimit) {
            scenesegments.push(list[i][1]);
            data.push([currentSceneTimeStamp, list[i][1], description]);
            description = " ";
            currentSceneTimeStamp = list[i][1];
          }
          firstSkip = false;
        }
        if (list[i][4] == "SKIP") {
          if (firstSkip == false) {
            firstSkip = true;
            skiptimestamp = list[i][1];
          }
        }
      }
      return data;
    };
    const sceneSegmentedData = SceneSegmentation(10, 0.75);
    stringify(
      sceneSegmentedData,
      { header: true, columns: columns },
      (err, output) => {
        if (err) throw err;
        fs.writeFile(scenesegmentedfile, output, (err) => {
          if (err) throw err;
          console.log(`${scenesegmentedfile} saved.`);
        });
      }
    );
  });
