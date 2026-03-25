# Install required package if you haven't already
# install.packages("asteRisk")
# install.packages("arrow")

library(asteRisk)
library(arrow)

# 1. Load the data
project_folder <- paste0(dirname(sys.frame(1)$ofile), "/")
df <- read_parquet(paste0(project_folder, "latest_satellites.parquet"))

# Ensure columns are character strings for the parser
df$TLE_LINE1 <- as.character(df$TLE_LINE1)
df$TLE_LINE2 <- as.character(df$TLE_LINE2)

# 2. Select your "High-Value Asset" (HVA) to protect. 
# Let's use the International Space Station (NORAD ID 25544 as an example)
# If your file doesn't have the ISS, just pick the first row for testing:
hva_line1 <- df$TLE_LINE1[1] 
hva_line2 <- df$TLE_LINE2[1]

# 3. Define the Prediction Window (e.g., 120 minutes into the future)
prediction_time_minutes <- 120

# Helper variables for Unit Conversions
deg2rad <- pi / 180
revPerDay2radPerMin <- (2 * pi) / 1440

# 4. Propagate the HVA (ISS) to get its future X, Y, Z coordinates
hva_tle <- parseTLElines(c(hva_line1, hva_line2)) 

# Map the descriptive names to SGP4 algebraic variables AND apply conversions
hva_future <- sgp4(
  n0 = hva_tle$meanMotion * revPerDay2radPerMin, 
  e0 = hva_tle$eccentricity, 
  i0 = hva_tle$inclination * deg2rad, 
  M0 = hva_tle$meanAnomaly * deg2rad, 
  omega0 = hva_tle$perigeeArgument * deg2rad, 
  OMEGA0 = hva_tle$ascension * deg2rad, 
  Bstar = hva_tle$Bstar,
  targetTime = prediction_time_minutes
)
hva_position <- hva_future$position 

print(paste("HVA Future Position (X,Y,Z km):", paste(round(hva_position, 2), collapse=", ")))

# 5. Rebuild the Distance Calculator Function
calculate_future_distance <- function(line1, line2) {
  tryCatch({
    target_tle <- parseTLElines(c(line1, line2))
    
    # Propagate the target satellite with mapped variables & conversions
    target_future <- sgp4(
      n0 = target_tle$meanMotion * revPerDay2radPerMin, 
      e0 = target_tle$eccentricity, 
      i0 = target_tle$inclination * deg2rad, 
      M0 = target_tle$meanAnomaly * deg2rad, 
      omega0 = target_tle$perigeeArgument * deg2rad, 
      OMEGA0 = target_tle$ascension * deg2rad, 
      Bstar = target_tle$Bstar,
      targetTime = prediction_time_minutes
    )
    target_pos <- target_future$position
    
    # Calculate 3D Euclidean Distance between the two satellites
    distance_km <- sqrt(sum((hva_position - target_pos)^2))
    return(distance_km)
    
  }, error = function(e) {
    # If the TLE is corrupted or missing data, skip it safely
    return(NA) 
  })
}

# 6. Apply the function to all satellites
print("Calculating collision risks (this may take a few seconds)...")
df$Future_Distance_to_HVA_km <- mapply(calculate_future_distance, df$TLE_LINE1, df$TLE_LINE2)

# 7. Print the top threats
threats <- subset(df, Future_Distance_to_HVA_km < 50)
threats <- threats[order(threats$Future_Distance_to_HVA_km), ]
print("Top Collision Threats in the next 2 hours:")
print(head(threats[, c("NORAD_CAT_ID", "Future_Distance_to_HVA_km")]))