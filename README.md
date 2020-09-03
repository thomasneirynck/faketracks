Creates fake tracks in real-time and adds them to Elasticsearch.

It uses a seed-file for geojson-lines (`.tracks.json`), along which the tracks are generated.


Install dependencies:

> npm install


Generate tracks:

> node ./generate_tracks.js