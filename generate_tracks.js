const {Client} = require('@elastic/elasticsearch');
const fs = require('fs');
const readline = require('readline');
const turf = require('@turf/turf');
const yargs = require('yargs')


const DEFAULT_TRACKS_JSON = 'tracks.json';
const DEFAULT_INDEX_NAME = 'tracks';
const DEFAULT_UPDATE_DELTA = 500;
const DEFAULT_SPEED = 100000 * 5;
const distanceUnit = 'miles';

const argv = yargs
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
    .help()
    .argv;

const trackFileName = argv.tracks;
const tracksIndexName = argv.index;
const updateDelta = argv.frequency; //milliseconds
const speedInUnitsPerHour = argv.speed; //units / per hour


const trackRaw = fs.readFileSync(trackFileName, 'utf-8');
const tracksFeatureCollection = JSON.parse(trackRaw);


const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const esClient = new Client({
    node: 'http://localhost:9200',
    // node: 'https://localhost:9200',
    auth: {
        username: 'elastic',
        password: 'changeme'
    },
    ssl: {
        rejectUnauthorized: false
    }
});

function initTrackMeta() {
    tracksFeatureCollection.features.forEach((track) => {
        track.properties.__length = turf.length(track.geometry, {units: distanceUnit});
        track.properties.__distanceTraveled = 0;
        track.properties.__reset = true;
    });
}

async function recreateIndex() {
    console.log(`Create index "${tracksIndexName}"`);
    await esClient.indices.create({
        index: tracksIndexName,
        body: {
            mappings: {
                "properties": {
                    'location': {
                        "type": 'geo_point',
                        "ignore_malformed": true
                    },
                    "entity_id": {
                        "type": "keyword"
                    },
                    "@timestamp": {
                        "type": "date"
                    }
                }
            }
        }
    });
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
                    console.log('re', response);
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
            console.error(e);
            reject(e);
        }
    });
}

let tickCounter = 0;

async function generateWaypoints() {

    console.log(`[${tickCounter}-------------- GENERATE WAYPOINTS AT TICK ${(new Date()).toISOString()}`);

    for (let i = 0; i < tracksFeatureCollection.features.length; i++) {

        const track = tracksFeatureCollection.features[i];
        const trackId = track.id || i;

        const timeStamp = new Date();


        let wayPointES;
        if (track.properties.__reset) {
            wayPointES = track.geometry.coordinates[0];
            track.properties.__reset = false;
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
                console.log('Reset track ' + trackId);
                track.properties.__reset = true;
                track.properties.__distanceTraveled = 0;
            }
            const wayPointFeature = turf.along(track.geometry, targetDistance, {units: distanceUnit});
            wayPointES = wayPointFeature.geometry.coordinates;
        }

        console.log(`\t update track: \t${trackId} - ${wayPointES}`)

        const doc = {
            location: wayPointES,
            entity_id: trackId,
            "@timestamp": timeStamp.toISOString(),
        };

        await esClient.index({
            index: tracksIndexName,
            body: doc
        });

        track.properties.__lastUpdate = timeStamp.getTime();
    }

    tickCounter++;
    setTimeout(generateWaypoints, updateDelta);

}

async function init() {
    initTrackMeta();
    await setupIndex();
    generateWaypoints();
}

init();