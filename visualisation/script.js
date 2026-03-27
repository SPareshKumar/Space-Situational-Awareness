// 1. Initialize the Cesium Viewer
const viewer = new Cesium.Viewer('cesiumContainer', {
    terrainProvider: Cesium.createWorldTerrain(),
    baseLayerPicker: false,
    shouldAnimate: true,
});

// 2. Load threat data, then satellite TLEs
let threatMap = {};

fetch('space_guardian_threats.json')
    .then(r => r.json())
    .catch(() => {
        console.warn("No threat data available, rendering all satellites as normal.");
        return [];
    })
    .then(threats => {
        threats.forEach(t => {
            threatMap[String(t.NORAD_CAT_ID)] = t;
        });
        console.log("Loaded " + Object.keys(threatMap).length + " threat entries.");
        return fetch('satellites.txt');
    })
    .then(response => response.text())
    .then(data => {
        const lines = data.split(/\r?\n/);
        for (let i = 0; i < lines.length; i += 2) {
            const line1 = lines[i]?.trim();
            const line2 = lines[i + 1]?.trim();

            if (line1 && line2 && line1.startsWith('1') && line2.startsWith('2')) {
                try {
                    addSatelliteToGlobe(line1, line2);
                } catch (err) {
                    console.warn("Skipping a satellite due to parsing error:", err);
                }
            }
        }
    })
    .catch(error => console.error("Error loading satellite data:", error));

function addSatelliteToGlobe(tle1, tle2) {
    const satrec = satellite.twoline2satrec(tle1, tle2);
    // satnum can be a number or string; let's force it to a clean string
    const noradId = String(satrec.satnum).replace(/^0+/, '').trim(); 
    const threat = threatMap[noradId] || null;

    // Default style: CYAN for normal satellites
    let color = Cesium.Color.CYAN.withAlpha(0.6);
    let size = 4;
    let label = 'Satellite ' + (noradId || 'Unknown');

    // If it's a known threat (from the R pipeline)
    if (threat) {
        const score = threat.Final_Threat_Score || 0;
        
        // Critical: Red
        if (score >= 70) {
            color = Cesium.Color.RED;
            size = 12;
            console.log("ALERT: Critical threat detected!", threat.OBJECT_NAME, "(score: " + score + ")");
        } 
        // Warning: Orange
        else if (score >= 40) {
            color = Cesium.Color.ORANGE;
            size = 10;
        } 
        // Monitor: Yellow
        else {
            color = Cesium.Color.YELLOW;
            size = 8;
        }
        label = (threat.OBJECT_NAME || label) + " [" + score.toFixed(0) + "]";
    }

    const entityOptions = {
        name: label,
        position: new Cesium.CallbackProperty(function(time) {
            const date = Cesium.JulianDate.toDate(time);
            const positionAndVelocity = satellite.propagate(satrec, date);

            if (!positionAndVelocity || !positionAndVelocity.position) {
                return undefined;
            }

            const positionEci = positionAndVelocity.position;
            const gmst = satellite.gstime(date);
            const positionGd = satellite.eciToGeodetic(positionEci, gmst);

            return Cesium.Cartesian3.fromRadians(
                positionGd.longitude,
                positionGd.latitude,
                positionGd.height * 1000
            );
        }, false),
        point: {
            pixelSize: size,
            color: color,
            outlineColor: Cesium.Color.BLACK,
            outlineWidth: 1
        }
    };

    // Add click-to-inspect info box for threat satellites
    if (threat) {
        const score = threat.Final_Threat_Score;
        const dist  = threat.Min_Conjunction_Distance_km;
        const anom  = threat.Anomaly_Score;
        const regime = threat.Orbit_Regime || 'N/A';
        const nearestNorad = threat.Nearest_Threat_NORAD || 'N/A';
        entityOptions.description =
            '<table style="width:100%; font-family:monospace;">' +
            '<tr><td><b>NORAD ID</b></td><td>' + threat.NORAD_CAT_ID + '</td></tr>' +
            '<tr><td><b>Object</b></td><td>' + (threat.OBJECT_NAME || 'Unknown') + '</td></tr>' +
            '<tr><td><b>Orbit Regime</b></td><td>' + regime + '</td></tr>' +
            '<tr><td><b>Conjunction Dist</b></td><td>' + (dist != null ? dist.toFixed(2) + ' km' : 'N/A') + '</td></tr>' +
            '<tr><td><b>Nearest Threat</b></td><td>NORAD ' + nearestNorad + '</td></tr>' +
            '<tr><td><b>Anomaly Score</b></td><td>' + (anom != null ? anom.toFixed(4) : 'N/A') + '</td></tr>' +
            '<tr><td><b>Threat Score</b></td><td>' + (score != null ? score.toFixed(1) : 'N/A') + '</td></tr>' +
            '</table>';
    }

    viewer.entities.add(entityOptions);
}
