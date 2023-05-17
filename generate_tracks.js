const {Client} = require('@elastic/elasticsearch');
const fs = require('fs');
const readline = require('readline');
const turf = require('@turf/turf');
const yargs = require('yargs')


const DEFAULT_TRACKS_JSON = 'manhattan_tracks.json';
const DEFAULT_INDEX_NAME = 'tracks';
const DEFAULT_UPDATE_DELTA = 1000; //ms
const DEFAULT_TIME_JIGGER = DEFAULT_UPDATE_DELTA * 3;
const DEFAULT_SPEED = 40; //mph
const DEFAULT_HOST = `http://localhost:9200`;
const DEFAULT_OMIT_RANDOM_FEATURE = false;
const DEFAULT_TIME_SERIES = false;
const distanceUnit = 'miles';

const argv = yargs
    .option('timeJigger', {
        alias: 'j',
        description: 'Delay time for time <j>',
        type: 'number',
        default: DEFAULT_TIME_JIGGER,
    })
    .option('tracks', {
        alias: 't',
        description: 'path to the tracks geojson file. This is a FeatureCollection with only linestrings',
        type: 'string',
        default: DEFAULT_TRACKS_JSON,
    })
    .option('index', {
        alias: 'i',
        description: 'name of the elasticsearch index',
        type: 'string',
        default: DEFAULT_INDEX_NAME,
    })
    .option('speed', {
        alias: 's',
        description: `speed of the track in ${distanceUnit}/hour`,
        type: 'number',
        default: DEFAULT_SPEED,
    })
    .option('frequency', {
        alias: 'f',
        description: `Update delta of the tracks in ms`,
        type: 'number',
        default: DEFAULT_UPDATE_DELTA,
    })
    .option('omitRandomFeature', {
        alias: 'o',
        description: 'Omits updates for a random feature for 3 cycles',
        type: 'boolean',
        default: DEFAULT_OMIT_RANDOM_FEATURE,
    })
    .option('host', {
        alias: 'h',
        description: 'URL of the elasticsearch server',
        type: 'string',
        default: DEFAULT_HOST,
    })
    .option('isTimeSeries', {
        alias: 'ts',
        description: 'When true, events stored in time series index with dimension: entity_id, metric: location',
        type: 'boolean',
        default: DEFAULT_TIME_SERIES,
    })
    .help()
    .argv;

const trackFileName = argv.tracks;
const tracksIndexName = argv.index;
const updateDelta = argv.frequency; //milliseconds
const timeJigger = argv.timeJigger;
const speedInUnitsPerHour = argv.speed; //units / per hour
const isTimeSeries = argv.isTimeSeries;

const trackRaw = fs.readFileSync(trackFileName, 'utf-8');
const tracksFeatureCollection = JSON.parse(trackRaw);

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const esClient = new Client({
    node: argv.host,
    auth: {
        username: 'elastic',
        password: 'changeme'
    },
    ssl: {
        rejectUnauthorized: false
    }
});

async function init() {
    initTrackMeta();
    await setupIndex();
    generateWaypoints();
}

init();

function initTrackMeta() {
    tracksFeatureCollection.features.forEach((track) => {
        track.properties.__length = turf.length(track.geometry, {units: distanceUnit});
        track.properties.__distanceTraveled = 0;
        track.properties.__reset = true;
        track.properties.__lastWayPoint = track.geometry.coordinates[0];
    });
}

async function recreateIndex() {
    console.log(`Create index "${tracksIndexName}"`);
    if (isTimeSeries) {
        console.log('time series dimension: entity_id, metric: location');
    }
    try {
        await esClient.indices.create({
            index: tracksIndexName,
            body: {
                mappings: {
                    properties: {
                        location: {
                            type: 'geo_point',
                            ignore_malformed: true,
                            ...(isTimeSeries ? { time_series_metric: 'position' } : {}),
                        },
                        entity_id: {
                            type: "keyword",
                            ...(isTimeSeries ? { time_series_dimension: true } : {}),
                        },
                        azimuth: {
                            type: "double"
                        },
                        speed: {
                            type: "double"
                        },
                        '@timestamp': {
                            "type": "date"
                        },
                        time_jigger: {
                            type: "date"
                        },
                    }
                }
            }
        });
    } catch (e) {
        console.error(e.body.error);
        throw e;
    }
}

async function setupIndex() {

    return new Promise(async (resolve, reject) => {

        try {
            await esClient.ping({});
        } catch (e) {
            console.error('Cannot reach Elasticsearch', e);
            reject(e);
        }

        try {

            const {body} = await esClient.indices.exists({
                index: tracksIndexName,
            });

            if (body) {
                rl.question(`Index "${tracksIndexName}" exists. Should delete and recreate? [n|Y]`, async function (response) {
                    if (response === 'y' || response === 'Y') {
                        console.log(`Deleting index "${tracksIndexName}"`);
                        await esClient.indices.delete({
                            index: tracksIndexName
                        });
                        await recreateIndex();
                    } else {
                        console.log('Retaining existing index');
                    }
                    resolve();
                });

            } else {
                await recreateIndex();
                resolve();
            }

        } catch (e) {
            console.error(e.message);
            reject(e);
        }
    });
}

let tickCounter = 0;
let cycleCounter = 4;
let featureToOmit = 0;

async function generateWaypoints() {

    console.log(`[${tickCounter}-------------- GENERATE ${tracksFeatureCollection.features.length} WAYPOINTS AT TICK ${(new Date()).toISOString()}`);

    if (argv.omitRandomFeature) {
        if (cycleCounter > 3) {
            cycleCounter = 1;
            featureToOmit = Math.floor(Math.random() * Math.floor(tracksFeatureCollection.features.length));
        }
        console.log(`Omitting feature: ${featureToOmit}, cycle: ${cycleCounter} of 3`);
        cycleCounter++;
    }


    const bulkInsert = [];

    for (let i = 0; i < tracksFeatureCollection.features.length; i++) {
        if (argv.omitRandomFeature) {
            if (i === featureToOmit) {
                continue;
            }
        }

        const track = tracksFeatureCollection.features[i];
        const trackId = track.id || i;

        const timeStamp = new Date();

        let wayPointES;
        if (track.properties.__reset) {
            track.geometry.coordinates.reverse();// make track drive other direction
            wayPointES = track.geometry.coordinates[0];

            if (tickCounter !== 0) {
                track.properties.__reset = false;
            }
        } else {
            const delta = timeStamp.getTime() - track.properties.__lastUpdate;
            const deltaInHours = delta / (1000 * 60 * 60);
            const deltaDistance = deltaInHours * speedInUnitsPerHour;
            const totalDistance = deltaDistance + track.properties.__distanceTraveled;

            let targetDistance;
            if (totalDistance < track.properties.__length) {
                targetDistance = totalDistance;
                track.properties.__distanceTraveled = targetDistance;
            } else {
                targetDistance = track.properties.__length;
                track.properties.__reset = true;
                track.properties.__distanceTraveled = 0;
            }
            const wayPointFeature = turf.along(track.geometry, targetDistance, {units: distanceUnit});
            wayPointES = wayPointFeature.geometry.coordinates;
        }

        //Use azimuth in web mercator for better display in Maps.
        const from = turf.toMercator(track.properties.__lastWayPoint);
        const to = turf.toMercator(wayPointES);
        const azimuth = azimuthInDegrees(from[0], from[1], to[0], to[1]);
        const timeJiggerIsoString = (new Date((Date.now() - timeJigger * Math.random()))).toISOString();

        const doc = {
            // azimuth: azimuth,
            azimuth: (azimuth * -1) + 90, // hack to use 2D semantics (probable bug in maps https://github.com/elastic/kibana/issues/77496)
            location: wayPointES,
            entity_id: trackId,
            speed: speedInUnitsPerHour,
            "@timestamp": timeStamp.toISOString(),
            time_jigger: timeJiggerIsoString,
        };

        track.properties.__lastUpdate = timeStamp.getTime();
        track.properties.__lastWayPoint = wayPointES.slice();

        bulkInsert.push({
            index: {
                _index: tracksIndexName,
            }
        });
        bulkInsert.push(doc);
    }

    tickCounter++;

    await esClient.bulk({
        body: bulkInsert
    });
    setTimeout(generateWaypoints, updateDelta);

}

function azimuthInDegrees(x1, y1, x2, y2) {
    return (Math.atan2(y2 - y1, x2 - x1) * 180 / Math.PI) ;
}