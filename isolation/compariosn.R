# 1. SETUP & LOAD DATA
library(arrow)
library(isotree)
library(ggplot2)
library(dplyr)

# Hardcode the exact path to prevent the 'closure' error
project_folder <- "C:/Projects/HWSW/major/isolation/"
df <- read_parquet(paste0(project_folder, "latest_satellites.parquet"))

print(paste("Successfully loaded", nrow(df), "satellites."))

# 2. TRAIN ISOLATION FOREST
features <- df[, c("INCLINATION", "ECCENTRICITY", "MEAN_MOTION", "BSTAR")]
features[is.na(features)] <- 0 # Quick clean for NAs

set.seed(42)
iso_model <- isolation.forest(features, ntrees=100, sample_size=256)
df$Anomaly_Score <- predict(iso_model, features)

# 3. CALCULATE ADAPTIVE THRESHOLD (Maximizing Variance)
find_adaptive_threshold <- function(scores) {
  breaks <- seq(min(scores), max(scores), length.out = 100)
  best_variance <- 0
  optimal_threshold <- median(scores)
  
  for (t in breaks) {
    normal_class <- scores[scores <= t]
    anomaly_class <- scores[scores > t]
    if(length(normal_class) == 0 || length(anomaly_class) == 0) next
    
    w_normal <- length(normal_class) / length(scores)
    w_anomaly <- length(anomaly_class) / length(scores)
    variance_between <- w_normal * w_anomaly * (mean(normal_class) - mean(anomaly_class))^2
    
    if (variance_between > best_variance) {
      best_variance <- variance_between
      optimal_threshold <- t
    }
  }
  return(optimal_threshold)
}

dynamic_threshold <- find_adaptive_threshold(df$Anomaly_Score)
print(paste(">>> OPTIMAL ADAPTIVE THRESHOLD:", round(dynamic_threshold, 4)))

# 4. GENERATE COMPARISON TABLE
fixed_thresholds <- c(0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70)

evaluate_threshold <- function(scores, t_value, t_name) {
  anomalies <- sum(scores > t_value)
  total <- length(scores)
  percentage <- (anomalies / total) * 100
  return(data.frame(Method = t_name, Threshold = round(t_value, 4), Anomalies_Found = anomalies, Anomaly_Percentage = round(percentage, 2)))
}

comparison_data <- data.frame()
for (t in fixed_thresholds) {
  comparison_data <- rbind(comparison_data, evaluate_threshold(df$Anomaly_Score, t, paste("Fixed Baseline", t)))
}
comparison_data <- rbind(comparison_data, evaluate_threshold(df$Anomaly_Score, dynamic_threshold, "Adaptive (Proposed)"))

# Sort table by threshold so it reads cleanly in your paper
comparison_data <- comparison_data[order(comparison_data$Threshold), ]
print("--- COMPARISON TABLE FOR IEEE PAPER ---")
print(comparison_data)

# 5. GENERATE SENSITIVITY GRAPH
graph_thresholds <- seq(0.35, 0.75, by=0.01)
graph_data <- data.frame(Threshold = graph_thresholds)
graph_data$Percentage <- sapply(graph_data$Threshold, function(t) (sum(df$Anomaly_Score > t) / nrow(df)) * 100)
adaptive_percentage <- (sum(df$Anomaly_Score > dynamic_threshold) / nrow(df)) * 100

sensitivity_plot <- ggplot(graph_data, aes(x=Threshold, y=Percentage)) +
  geom_line(color="blue", size=1) +
  geom_point(aes(x=dynamic_threshold, y=adaptive_percentage), color="red", size=4, shape=18) +
  geom_vline(xintercept=dynamic_threshold, color="red", linetype="dashed") +
  annotate("text", x=dynamic_threshold + 0.06, y=adaptive_percentage + 15, 
           label=paste("Adaptive Optimal\nThreshold =", round(dynamic_threshold, 3)), color="red", fontface="bold") +
  theme_bw() +
  labs(title="Threshold Sensitivity Analysis: Isolation Forest",
       x="Anomaly Score Threshold", 
       y="Percentage of Fleet Classified as Anomalous (%)") +
  theme(text = element_text(family = "serif", size=12),
        plot.title = element_text(face="bold", hjust=0.5))

print(sensitivity_plot)

# Save the model
saveRDS(iso_model, paste0(project_folder, "isolation_model.rds"))