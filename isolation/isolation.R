library(arrow)
library(isotree)
library(ggplot2)

# 1. Load the Parquet file you generated from Python
df <- read_parquet("C:/Projects/HWSW/major/isolation/latest_satellites.parquet")

# 2. Select the features that define "Orbital Physics"
# We drop IDs and Names because the model only cares about numbers
features <- df[, c("INCLINATION", "ECCENTRICITY", "MEAN_MOTION", "BSTAR")]

# 3. Handle any missing values (Isolation Forest can handle NAs, but it's best to clean)
features <- na.omit(features)

# Train the model 
# ntrees=100 is standard. sample_size=256 creates the optimal tree depth.
set.seed(42) # For reproducibility
iso_model <- isolation.forest(features, ntrees=100, sample_size=256)

# Generate the Anomaly Scores (closer to 1 = more anomalous)
scores <- predict(iso_model, features)
df$Anomaly_Score <- scores

# Function to find the optimal dynamic threshold based on variance
find_adaptive_threshold <- function(scores) {
  # Create 100 possible threshold points between the min and max scores
  breaks <- seq(min(scores), max(scores), length.out = 100)
  
  best_variance <- 0
  optimal_threshold <- median(scores)
  
  for (t in breaks) {
    # Split scores into two classes based on current threshold 't'
    normal_class <- scores[scores <= t]
    anomaly_class <- scores[scores > t]
    
    # Skip if a class is empty
    if(length(normal_class) == 0 || length(anomaly_class) == 0) next
    
    # Calculate weights (proportion of data in each class)
    w_normal <- length(normal_class) / length(scores)
    w_anomaly <- length(anomaly_class) / length(scores)
    
    # Calculate Between-Class Variance
    variance_between <- w_normal * w_anomaly * (mean(normal_class) - mean(anomaly_class))^2
    
    # Maximize the variance
    if (variance_between > best_variance) {
      best_variance <- variance_between
      optimal_threshold <- t
    }
  }
  
  return(optimal_threshold)
}

# Apply the algorithm to our scores
dynamic_threshold <- find_adaptive_threshold(df$Anomaly_Score)
print(paste("The dynamically calculated threshold is:", round(dynamic_threshold, 4)))

# Classify the satellites based on the adaptive threshold
df$Status <- ifelse(df$Anomaly_Score > dynamic_threshold, "Anomalous", "Normal")

# Check how many anomalies were found
table(df$Status)

# Plot the distribution to visualize how well the threshold worked
ggplot(df, aes(x=Anomaly_Score, fill=Status)) +
  geom_histogram(bins=50, alpha=0.7, position="identity") +
  geom_vline(xintercept=dynamic_threshold, color="red", linetype="dashed", size=1) +
  theme_minimal() +
  labs(title="Space Guardian: Adaptive Isolation Forest Scores",
       x="Anomaly Score", 
       y="Number of Satellites") +
  scale_fill_manual(values=c("Normal"="cyan", "Anomalous"="red"))

library(isotree)

iso_model <- isolation.forest(df)

saveRDS(iso_model, "C:/Projects/HWSW/isolation/isolation_model.rds")
