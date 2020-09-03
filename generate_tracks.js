const {Client} = require('@elastic/elasticsearch');
const fs = require('fs');
const turf = require('@turf/turf');

const trackFileName = 'tracks.json';

const trackRaw = fs.readFileSync(trackFileName, 'utf-8');
const tracksFeatureCollection = JSON.parse(trackRaw);

const tracksIndexName = 'tracks';

const distanceUnit = 'miles';
const updateDelta = 2000;
const speed =  10000; //per hour

const esClient = new Client({
    node: 'http://localhost:9200',
    auth: {
        username: 'elastic',
        password: 'changeme'
    }
});

function initTrackMeta() {
    tracksFeatureCollection.features.forEach((track) => {
        track.properties.__length = turf.length(track.geometry, { units: distanceUnit});
        track.properties.__distanceTraveled = 0;
        track.properties.__reset = true;

    });
}


async function generateTracks() {
    try {
        await esClient.ping({
            // requestTimeout: 1000
        });
    } catch (e) {
        console.error('Cannot reach Elasticsearch', e);
        throw e;
    }

    try {
        await esClient.indices.delete({
            index: tracksIndexName
        });
    } catch (e) {
        console.warn(e);
    }

    try {
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
    } catch (e) {
        console.error(e);
        throw e;
    }

    generateWaypoints();
}

let tickCounter = 0;
let idCounter = 0;
async function generateWaypoints() {

    console.log(`[${tickCounter}-------------- GENERATE WAYPOINTS AT TICK ${isoDate()}`);

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
            const deltaDistance = deltaInHours * speed;
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

        console.log(`update track: \t${trackId} - ${wayPointES}`)

        const doc = {
            location: wayPointES,
            entity_id: trackId,
            "@timestamp": timeStamp.toISOString(),
        };

        await esClient.create({
            id: idCounter++,
            index: tracksIndexName,
            body: doc
        });

        track.properties.__lastUpdate = timeStamp.getTime();
    }

    tickCounter++;
    setTimeout(generateWaypoints, updateDelta);

}

function generateTrackLocation() {

}

function isoDate() {
    return (new Date()).toISOString();
}

function calculateTrackLengths() {

}

initTrackMeta();
generateTracks();