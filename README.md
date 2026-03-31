# 🚀 Space Guardian: A Cybersecurity-Aware Space Situational Awareness System with Adaptive Isolation Forest-Based Anomaly Detection

Space Guardian is an automated Space Situational Awareness (SSA) system designed to detect, analyze, and prioritize potential orbital threats using a cybersecurity-inspired pipeline. It combines anomaly detection, orbital mechanics, and soft computing to deliver intelligent, real-time threat assessment.

---

## 📌 Overview

The rapid increase in satellites and orbital debris has made space monitoring more complex than ever. Traditional SSA systems rely on static thresholds and computationally heavy processes, making them inefficient and incapable of detecting cyber-physical threats.

**Space Guardian addresses this by:**
- Automating data ingestion and analysis
- Detecting anomalies using machine learning
- Incorporating cybersecurity principles into SSA
- Providing intelligent threat prioritization instead of binary alerts

---

## 🧠 Core Features

### 🔍 1. Adaptive Anomaly Detection
- Uses **Adaptive Isolation Forest**
- Identifies unusual orbital behavior from raw satellite data
- Works in an **unsupervised** manner (no labeled data required)

### 🛰️ 2. Orbital Prediction
- Implements **SGP4 propagation model**
- Predicts future satellite positions and distances

### ⚖️ 3. Intelligent Threat Scoring
- Combines anomaly score + spatial proximity
- Uses **Fuzzy Inference System (FIS)**
- Outputs a **Threat Score (0–100)** instead of binary alerts

### 📊 4. Dynamic Threshold Optimization
- Uses **Otsu’s Method** to compute optimal anomaly threshold
- Eliminates manual tuning
- Adapts to changing orbital environments

### ⚡ 5. High-Performance Data Layer
- Uses **Apache Parquet (columnar storage)**
- Achieves:
  - ~88% storage reduction
  - ~30× faster data access compared to CSV

---

## 📜 License

This project is intended for research and academic purposes.
