const fs = require("fs");
const path = require("path");
const converter = require("json2csv");
const ConvertToCSV = require("json2csv-stream");
const readline = require("readline");
const { Readable, Writable, pipeline } = require("stream");
const { promisify } = require("util");
const { split } = require("event-stream");

const main = () => {
  const files = fs
    .readdirSync("./20221129")
    .filter((file) => path.extname(file) === ".json");

  const results = [];

  for (const file of files) {
    const jsonsInFile = fs
      .readFileSync(path.join("./20221129", file))
      .toString()
      .split("\n")
      .filter((line) => line.trim() !== "");

    for (const json of jsonsInFile) {
      // console.log(json, JSON.parse(json));
      results.push(JSON.parse(json));
    }
  }

  const csv = converter.parse(results);

  fs.writeFileSync("result.csv", csv);
};

const pipelineAsync = promisify(pipeline);

const loadWithStreams = async () => {
  const files = fs
    .readdirSync("./source")
    .filter((file) => path.extname(file) === ".json");

  const writeToCsv = fs.createWriteStream("out.csv");

  writeToCsv.write(
    `$insert_id, $insert_key, $schema, adid, amplitude_attribution_ids, amplitude_event_type, amplitude_id, app, city, client_event_time, client_upload_time, country, data, data_type, device_brand, device_carrier, device_family, device_id, device_manufacturer, device_model, device_type, dma, event_id, event_properties, event_time, event_type, global_user_properties, group_properties, groups, idfa, ip_address, is_attribution_event, language, library, location_lat, location_lng, os_name, os_version, partner_id, paying, plan, platform, processed_time, region, sample_rate, server_received_time, server_upload_time, session_id, source_id, start_version, user_creation_time, user_id, user_properties, uuid, version_name\n`
  );

  for (const index in files) {
    console.info("WRITING OF ------>", `${index} of ${files.length}`);
    const file = files[index];

    const writable = new Writable({
      write: function (data, encoding, cb) {
        if (data.toString() && data.toString().length > 0) {
          const obj = JSON.parse(data.toString());
          const csv = converter.parse(obj, { header: false });

          console.log("Got it!");
          writeToCsv.write(csv);
          writeToCsv.write("\n");
        }

        cb();
      },
    });

    const stream = fs
      .createReadStream(path.join("./source", file))
      .pipe(split());

    await pipelineAsync(stream, writable);
  }
};

loadWithStreams().catch(console.error);
