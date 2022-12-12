const fs = require("fs");
const { WritableStream } = require("node:stream/web");

const load = async () => {
  const uri =
    "https://amplitude.com/api/2/export?start=20221128T00&end=20221128T23";

  const key = "PUT_KEY_HERE";

  const response = await fetch(uri, {
    headers: {
      Authorization: `Basic ${key}`,
    },
  });

  const streamToPath = fs.createWriteStream("response.zip");
  const writable = new WritableStream({
    write(chunk) {
      streamToPath.write(chunk);
    },
  });

  await response.body.pipeTo(writable);
};

load().catch(console.error);
