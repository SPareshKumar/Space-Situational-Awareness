# =========================================================================
# SPACE GUARDIAN: MASTER PIPELINE (R SCRIPT)
# =========================================================================

# -------------------------------------------------------------------------
# 0. SETUP & LIBRARIES
# -------------------------------------------------------------------------
library(arrow)
library(isotree)
library(asteRisk)
library(sets)
library(jsonlite)

print("Starting Space Guardian Pipeline...")

# Portable path resolution (works on any OS when run via Rscript or source())
project_folder <- paste0(dirname(sys.frame(1)$ofile), "/")
input_file <- paste0(project_folder, "latest_satellites.parquet")
output_file <- paste0(project_folder, "space_guardian_threats.json")

# -------------------------------------------------------------------------
# 1. DATA INGESTION
# -------------------------------------------------------------------------
print(paste("Reading database from:", input_file))
df <- read_parquet(input_file)

# Ensure TLE lines are characters for the parser
df$TLE_LINE1 <- as.character(df$TLE_LINE1)
df$TLE_LINE2 <- as.character(df$TLE_LINE2)

print(paste("Loaded", nrow(df), "satellites from database."))

# -------------------------------------------------------------------------
# 2. SECURITY LAYER: ISOLATION FOREST
# -------------------------------------------------------------------------
print("Running Unsupervised Anomaly Detection...")

# Select the orbital physics features
features <- df[, c("INCLINATION", "ECCENTRICITY", "MEAN_MOTION", "BSTAR")]
features[is.na(features)] <- 0 # Handle any rare missing values

# Train the forest and predict anomaly scores
set.seed(42)
iso_model <- isolation.forest(features, ntrees=100, sample_size=256)
df$Anomaly_Score <- predict(iso_model, features)

# -------------------------------------------------------------------------
# 3. PHYSICS LAYER: SGP4 CONJUNCTION ASSESSMENT
# -------------------------------------------------------------------------
print("Propagating orbits and calculating future distances...")

# Unit Conversions required for SGP4 engine
deg2rad <- pi / 180
revPerDay2radPerMin <- (2 * pi) / 1440
prediction_time_minutes <- 120

# Define High-Value Asset (HVA) - Using the 1st row as our target (e.g., Vanguard/ISS)
hva_line1 <- df$TLE_LINE1[1]
hva_line2 <- df$TLE_LINE2[1]

hva_tle <- parseTLElines(c(hva_line1, hva_line2))
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

# Distance Calculator Function
calculate_future_distance <- function(line1, line2) {
  tryCatch({
    target_tle <- parseTLElines(c(line1, line2))
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
    # 3D Euclidean Distance
    return(sqrt(sum((hva_position - target_future$position)^2)))
  }, error = function(e) { return(NA) })
}

# Apply distance calculation to all satellites
df$Future_Distance_to_HVA_km <- mapply(calculate_future_distance, df$TLE_LINE1, df$TLE_LINE2)

# Filter for relevant physical threats (e.g., within 50km)
threats <- subset(df, Future_Distance_to_HVA_km < 50)
threats <- threats[order(threats$Future_Distance_to_HVA_km), ]

print(paste("Found", nrow(threats), "satellites in close proximity."))

# -------------------------------------------------------------------------
# 4. INTELLIGENCE LAYER: FUZZY LOGIC DECISION ENGINE
# -------------------------------------------------------------------------
print("Evaluating Multi-Domain Threat Levels...")

sets_options("universe", NULL)

# Define the Fuzzy Variables
variables <- set(
  Distance = fuzzy_partition(varnames = c(Close = 0, Medium = 50, Far = 100), sd = 20),
  Anomaly = fuzzy_partition(varnames = c(Normal = 0, Suspicious = 0.5, Malicious = 1.0), sd = 0.2),
  Threat = fuzzy_partition(varnames = c(Low = 10, Warning = 50, Critical = 90), sd = 15)
)

# Define the Logic Rules (Improved sensitivity)
rules <- set(
  fuzzy_rule(Distance %is% Close && Anomaly %is% Malicious, Threat %is% Critical),
  fuzzy_rule(Distance %is% Close && Anomaly %is% Suspicious, Threat %is% Critical), # Suspicious + Close = Critical
  fuzzy_rule(Distance %is% Close && Anomaly %is% Normal, Threat %is% Warning),
  fuzzy_rule(Distance %is% Medium && Anomaly %is% Malicious, Threat %is% Critical),  # Malicious + Medium = Critical
  fuzzy_rule(Distance %is% Medium && Anomaly %is% Suspicious, Threat %is% Warning),
  fuzzy_rule(Distance %is% Far && Anomaly %is% Malicious, Threat %is% Warning),     # Malicious even if Far = Warning
  fuzzy_rule(Distance %is% Far, Threat %is% Low),
  fuzzy_rule(Distance %is% Medium && Anomaly %is% Normal, Threat %is% Low)
)

space_guardian_fis <- fuzzy_system(variables, rules)

# Calculation Function
calculate_threat <- function(dist, anom) {
  if(is.na(dist) || is.na(anom)) return(0)
  
  # Ensure inputs are capped to the universe of discourse
  dist_val <- min(max(dist, 0), 100)
  anom_val <- min(max(anom, 0), 1.0)
  
  inference <- fuzzy_inference(space_guardian_fis, list(Distance = dist_val, Anomaly = anom_val))
  
  # Defuzzify: if inference is empty or invalid, return 0
  res <- tryCatch({
    val <- gset_defuzzify(inference, "centroid")
    if(is.nan(val) || is.na(val)) return(0)
    return(round(val, 1))
  }, error = function(e) { 
    return(0) 
  })
  
  return(res)
}

# Apply Fuzzy Logic only to the subsetted physical threats
threats$Final_Threat_Score <- mapply(calculate_threat, threats$Future_Distance_to_HVA_km, threats$Anomaly_Score)

# -------------------------------------------------------------------------
# 5. EXPORT LAYER: JSON GENERATION
# -------------------------------------------------------------------------
print("Exporting actionable intelligence to JSON...")

# Select only the columns needed by the CesiumJS frontend
export_data <- threats[, c("NORAD_CAT_ID", "OBJECT_NAME", "TLE_LINE1", "TLE_LINE2", 
                           "Future_Distance_to_HVA_km", "Anomaly_Score", "Final_Threat_Score")]

json_data <- toJSON(export_data, pretty = TRUE, na = "null")

# Save pipeline output
write(json_data, output_file)

print("=========================================================")
print(paste("PIPELINE COMPLETE! Data saved to:", output_file))
print("=========================================================")
print(head(threats[, c("NORAD_CAT_ID", "Future_Distance_to_HVA_km", "Anomaly_Score", "Final_Threat_Score")]))