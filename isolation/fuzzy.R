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

# Portable path resolution (works with both Rscript and source())
args <- commandArgs(trailingOnly = FALSE)
script_arg <- args[grep("--file=", args)]
if (length(script_arg) > 0) {
  project_folder <- paste0(dirname(normalizePath(sub("--file=", "", script_arg))), "/")
} else {
  project_folder <- paste0(dirname(normalizePath(sys.frame(1)$ofile)), "/")
}
input_file  <- paste0(project_folder, "latest_satellites.parquet")
output_file <- paste0(project_folder, "space_guardian_threats.json")

# -------------------------------------------------------------------------
# 1. DATA INGESTION
# -------------------------------------------------------------------------
print(paste("Reading database from:", input_file))
df <- read_parquet(input_file)
df$TLE_LINE1 <- as.character(df$TLE_LINE1)
df$TLE_LINE2 <- as.character(df$TLE_LINE2)
print(paste("Loaded", nrow(df), "satellites from database."))

# -------------------------------------------------------------------------
# 2. SECURITY LAYER: ISOLATION FOREST (all satellites)
# -------------------------------------------------------------------------
print("Running Unsupervised Anomaly Detection...")
features <- df[, c("INCLINATION", "ECCENTRICITY", "MEAN_MOTION", "BSTAR")]
features[is.na(features)] <- 0
set.seed(42)
iso_model <- isolation.forest(features, ntrees = 100, sample_size = 256)
df$Anomaly_Score <- predict(iso_model, features)

# -------------------------------------------------------------------------
# 3. ORBIT REGIME CLASSIFICATION
# -------------------------------------------------------------------------
df$Orbit_Regime <- ifelse(df$MEAN_MOTION > 11, "LEO",
                   ifelse(df$MEAN_MOTION > 0.9 & df$MEAN_MOTION < 1.1, "GEO", "MEO"))

df$Threat_Threshold <- ifelse(df$Orbit_Regime == "LEO",  75,
                       ifelse(df$Orbit_Regime == "GEO",  20, 150))

# -------------------------------------------------------------------------
# 4. PHYSICS LAYER: SGP4 — propagate ALL satellites
# -------------------------------------------------------------------------
print("Propagating all orbits to future positions (120 min)...")

deg2rad            <- pi / 180
revPerDay2radPerMin <- (2 * pi) / 1440
prediction_time_minutes <- 120

propagate_to_position <- function(line1, line2) {
  tryCatch({
    tle    <- parseTLElines(c(line1, line2))
    result <- sgp4(
      n0     = tle$meanMotion       * revPerDay2radPerMin,
      e0     = tle$eccentricity,
      i0     = tle$inclination      * deg2rad,
      M0     = tle$meanAnomaly      * deg2rad,
      omega0 = tle$perigeeArgument  * deg2rad,
      OMEGA0 = tle$ascension        * deg2rad,
      Bstar  = tle$Bstar,
      targetTime = prediction_time_minutes
    )
    return(result$position)
  }, error = function(e) { return(c(NA_real_, NA_real_, NA_real_)) })
}

positions_list <- mapply(propagate_to_position, df$TLE_LINE1, df$TLE_LINE2, SIMPLIFY = FALSE)
pos_matrix     <- do.call(rbind, positions_list)   # N x 3 ECI positions (km)
print("Orbit propagation complete.")

# -------------------------------------------------------------------------
# 5. CONJUNCTION ANALYSIS: top-N anomalous vs same-regime neighbours
# -------------------------------------------------------------------------
print("Running regime-aware conjunction analysis...")

# Max anomalous candidates to check per regime
TOP_N <- list(LEO = 500, GEO = 100, MEO = 200)

df$Min_Conjunction_Distance_km <- NA_real_
df$Nearest_Threat_Anomaly      <- NA_real_
df$Nearest_Threat_NORAD        <- NA_integer_

for (regime in c("LEO", "GEO", "MEO")) {
  regime_idx <- which(df$Orbit_Regime == regime)
  if (length(regime_idx) == 0) next

  threshold  <- df$Threat_Threshold[regime_idx[1]]
  top_count  <- min(TOP_N[[regime]], length(regime_idx))

  # Pick top-N most anomalous satellites in this regime as candidates
  regime_anomaly     <- df$Anomaly_Score[regime_idx]
  cand_local_idx     <- order(regime_anomaly, decreasing = TRUE)[1:top_count]
  cand_global_idx    <- regime_idx[cand_local_idx]

  regime_positions   <- pos_matrix[regime_idx, , drop = FALSE]
  cand_positions     <- pos_matrix[cand_global_idx, , drop = FALSE]

  print(paste(regime, ": checking", top_count, "candidates against",
              length(regime_idx), "satellites"))

  # Distance matrix: candidates (rows) x all regime satellites (cols)
  dist_matrix <- matrix(NA_real_, nrow = top_count, ncol = length(regime_idx))

  for (k in seq_len(top_count)) {
    cand_pos <- cand_positions[k, ]
    if (any(is.na(cand_pos))) next
    diffs            <- regime_positions - matrix(cand_pos, nrow = nrow(regime_positions),
                                                   ncol = 3, byrow = TRUE)
    dist_matrix[k, ] <- sqrt(rowSums(diffs^2))
  }

  # Mask self-distances so candidates are not their own nearest neighbour
  for (k in seq_len(top_count)) {
    self_local <- which(regime_idx == cand_global_idx[k])
    if (length(self_local) > 0) dist_matrix[k, self_local] <- Inf
  }

  # For each regime satellite: find minimum distance across all candidates
  min_distances       <- apply(dist_matrix, 2, function(col) min(col, na.rm = TRUE))
  best_candidate_local <- apply(dist_matrix, 2, function(col) {
    m <- which.min(col); if (length(m) == 0) return(NA_integer_); m
  })

  # Keep only satellites within threshold
  nearby_mask        <- is.finite(min_distances) & min_distances < threshold
  nearby_regime_local <- which(nearby_mask)
  if (length(nearby_regime_local) == 0) next

  nearby_global       <- regime_idx[nearby_regime_local]
  best_cand_global    <- cand_global_idx[best_candidate_local[nearby_regime_local]]

  df$Min_Conjunction_Distance_km[nearby_global] <- min_distances[nearby_regime_local]
  df$Nearest_Threat_Anomaly[nearby_global]      <- df$Anomaly_Score[best_cand_global]
  df$Nearest_Threat_NORAD[nearby_global]        <- df$NORAD_CAT_ID[best_cand_global]
}

threats <- subset(df, !is.na(Min_Conjunction_Distance_km))
threats <- threats[order(threats$Min_Conjunction_Distance_km), ]
threats$Scaled_Distance <- (threats$Min_Conjunction_Distance_km / threats$Threat_Threshold) * 100

print(paste("Found", nrow(threats), "satellites in conjunction events."))

# -------------------------------------------------------------------------
# 6. INTELLIGENCE LAYER: FUZZY LOGIC DECISION ENGINE
# -------------------------------------------------------------------------
print("Evaluating Multi-Domain Threat Levels...")

sets_options("universe", NULL)

variables <- set(
  Distance = fuzzy_partition(varnames = c(Close = 0, Medium = 50, Far = 100), sd = 20),
  Anomaly  = fuzzy_partition(varnames = c(Normal = 0, Suspicious = 0.5, Malicious = 1.0), sd = 0.2),
  Threat   = fuzzy_partition(varnames = c(Low = 10, Warning = 50, Critical = 90), sd = 15)
)

rules <- set(
  fuzzy_rule(Distance %is% Close  && Anomaly %is% Malicious,  Threat %is% Critical),
  fuzzy_rule(Distance %is% Close  && Anomaly %is% Suspicious, Threat %is% Critical),
  fuzzy_rule(Distance %is% Close  && Anomaly %is% Normal,     Threat %is% Warning),
  fuzzy_rule(Distance %is% Medium && Anomaly %is% Malicious,  Threat %is% Critical),
  fuzzy_rule(Distance %is% Medium && Anomaly %is% Suspicious, Threat %is% Warning),
  fuzzy_rule(Distance %is% Far    && Anomaly %is% Malicious,  Threat %is% Warning),
  fuzzy_rule(Distance %is% Far    && Anomaly %is% Normal,     Threat %is% Low),
  fuzzy_rule(Distance %is% Far    && Anomaly %is% Suspicious, Threat %is% Low),
  fuzzy_rule(Distance %is% Medium && Anomaly %is% Normal,     Threat %is% Low)
)

space_guardian_fis <- fuzzy_system(variables, rules)

calculate_threat <- function(dist, anom) {
  if (is.na(dist) || is.na(anom)) return(0)
  dist_val <- min(max(dist, 0), 100)
  anom_val <- min(max(anom, 0), 1.0)
  inference <- fuzzy_inference(space_guardian_fis, list(Distance = dist_val, Anomaly = anom_val))
  res <- tryCatch({
    val <- gset_defuzzify(inference, "centroid")
    if (is.nan(val) || is.na(val)) return(0)
    return(round(val, 1))
  }, error = function(e) { return(0) })
  return(res)
}

# Fuzzy inputs: scaled distance + anomaly score of the nearest threatening candidate
threats$Final_Threat_Score <- mapply(calculate_threat,
                                     threats$Scaled_Distance,
                                     threats$Nearest_Threat_Anomaly)

# -------------------------------------------------------------------------
# 7. EXPORT LAYER: JSON GENERATION
# -------------------------------------------------------------------------
print("Exporting actionable intelligence to JSON...")

export_data <- threats[, c("NORAD_CAT_ID", "OBJECT_NAME", "TLE_LINE1", "TLE_LINE2",
                           "Min_Conjunction_Distance_km", "Anomaly_Score",
                           "Final_Threat_Score", "Orbit_Regime",
                           "Threat_Threshold", "Nearest_Threat_NORAD")]

json_data <- toJSON(export_data, pretty = TRUE, na = "null")
write(json_data, output_file)

print("=========================================================")
print(paste("PIPELINE COMPLETE!", nrow(export_data), "threats saved to:", output_file))
print("=========================================================")
print(head(threats[, c("NORAD_CAT_ID", "Orbit_Regime",
                       "Min_Conjunction_Distance_km", "Anomaly_Score", "Final_Threat_Score")]))
