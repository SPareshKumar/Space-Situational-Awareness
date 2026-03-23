// 1. Initialize the Cesium Viewer
const viewer = new Cesium.Viewer('cesiumContainer', {
    terrainProvider: Cesium.createWorldTerrain(),
    baseLayerPicker: false,
    shouldAnimate: true,
});

// 2. Fetch the satellite data file
fetch('satellites.txt')
    .then(response => response.text())
    .then(data => {
        const lines = data.split(/\r?\n/);
        // Loop through the file in pairs (Line 1 and Line 2)
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
    .catch(error => console.error("Error loading satellites.txt:", error));

function addSatelliteToGlobe(tle1, tle2) {
    // Initialize satellite record
    const satrec = satellite.twoline2satrec(tle1, tle2);

    viewer.entities.add({
        name: 'Satellite ' + (satrec.satnum || 'Unknown'),
        position: new Cesium.CallbackProperty(function(time) {
            const date = Cesium.JulianDate.toDate(time);
            
            // Propagate satellite position
            const positionAndVelocity = satellite.propagate(satrec, date);
            
            // ERROR FIX: Check if position exists before accessing .x, .y, or .z
            if (!positionAndVelocity || !positionAndVelocity.position) {
                return undefined; 
            }

            const positionEci = positionAndVelocity.position;
            const gmst = satellite.gstime(date);
            const positionGd = satellite.eciToGeodetic(positionEci, gmst);

            // Convert radians to Cesium Cartesian3
            return Cesium.Cartesian3.fromRadians(
                positionGd.longitude,
                positionGd.latitude,
                positionGd.height * 1000 // km to meters
            );
        }, false),
        point: {
            pixelSize: 5,
            color: Cesium.Color.CYAN,
            outlineColor: Cesium.Color.BLACK,
            outlineWidth: 1
        }
    });
}