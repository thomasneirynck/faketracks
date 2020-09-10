Creates fake tracks in real-time and adds them to Elasticsearch.

It uses a seed-file for geojson-lines (`tracks.json`), along which the tracks are generated.


Install dependencies:

> npm install


Generate tracks:

> node ./generate_tracks.js


Show docs:

> node ./generate_tracks.js --help

```
 Options:
       --version    Show version number                                 [boolean]
   -t, --tracks     path to the tracks geojson file. This is a FeatureCollection
                    with only linestrings       [string] [default: "tracks.json"]
   -i, --index      name of the elasticsearch index  [string] [default: "tracks"]
   -s, --speed      speed of the track in miles/hour   [number] [default: 500000]
   -f, --frequency  Update delta of the tracks in ms      [number] [default: 500]
       --help       Show help                                           [boolean]
```