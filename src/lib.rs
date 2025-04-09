mod cleaning;
mod stats;
mod tui;
mod types;

use cleaning::{check_consistency, detect_duplicates, detect_outliers, detect_redundancy};
use rayon::prelude::*;
use stats::{
    ColumnStats, compute_cardinality, compute_dependency_scores, compute_distribution,
    detect_drift, detect_temporal_patterns, estimate_noise, suggest_transformations,
};
use thiserror::Error;
pub use tui::render_tui;
use types::TypeInference;

#[derive(Debug, Error)]
pub enum PrestoError {
    #[error("Empty dataset provided")]
    EmptyDataset,
    #[error("Invalid numeric data: {0}")]
    InvalidNumeric(String),
}

#[derive(Debug, Clone)]
pub struct Dataset {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Dataset {
    pub fn new(headers: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Dataset { headers, rows }
    }

    pub fn from_csv(path: &str) -> Result<Self, PrestoError> {
        let mut rdr =
            csv::Reader::from_path(path).map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
        let headers = rdr
            .headers()
            .map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?
            .iter()
            .map(String::from)
            .collect();
        let rows: Vec<Vec<String>> = rdr
            .records()
            .map(|r| {
                let record = r.map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
                Ok(record.iter().map(String::from).collect::<Vec<String>>())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Dataset { headers, rows })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Description {
    stats: Vec<ColumnStats>,
    missing: Vec<usize>,
    duplicates: usize,
    outliers: Vec<Vec<usize>>,
    types: Vec<TypeInference>,
    dependency_scores: Vec<f64>,
    drift_scores: Vec<f64>,
    cardinality: Vec<usize>,
    distributions: Vec<Vec<(f64, usize)>>,
    consistency_issues: Vec<usize>,
    temporal_patterns: Vec<String>,
    transform_suggestions: Vec<String>,
    noise_scores: Vec<f64>,
    redundancy_pairs: Vec<(usize, usize, f64)>,
    total_rows: usize,
    missing_pct: f64,
    unique_pct: f64,
    top_values: Vec<(String, Vec<(String, usize)>)>,
    correlations: Vec<Vec<f64>>,
    feature_importance: Vec<(usize, f64)>,
    anomalies: Vec<(usize, f64, usize)>,
}

impl Description {
    pub fn new(
        stats: Vec<ColumnStats>,
        missing: Vec<usize>,
        duplicates: usize,
        outliers: Vec<Vec<usize>>,
        types: Vec<TypeInference>,
        dependency_scores: Vec<f64>,
        drift_scores: Vec<f64>,
        cardinality: Vec<usize>,
        distributions: Vec<Vec<(f64, usize)>>,
        consistency_issues: Vec<usize>,
        temporal_patterns: Vec<String>,
        transform_suggestions: Vec<String>,
        noise_scores: Vec<f64>,
        redundancy_pairs: Vec<(usize, usize, f64)>,
        total_rows: usize,
        missing_pct: f64,
        unique_pct: f64,
        top_values: Vec<(String, Vec<(String, usize)>)>,
        correlations: Vec<Vec<f64>>,
        feature_importance: Vec<(usize, f64)>,
        anomalies: Vec<(usize, f64, usize)>,
    ) -> Self {
        Description {
            stats,
            missing,
            duplicates,
            outliers,
            types,
            dependency_scores,
            drift_scores,
            cardinality,
            distributions,
            consistency_issues,
            temporal_patterns,
            transform_suggestions,
            noise_scores,
            redundancy_pairs,
            total_rows,
            missing_pct,
            unique_pct,
            top_values,
            correlations,
            feature_importance,
            anomalies,
        }
    }
}

pub fn describe(dataset: &Dataset) -> Result<Description, PrestoError> {
    if dataset.rows.is_empty() {
        return Err(PrestoError::EmptyDataset);
    }

    let num_cols = dataset.headers.len();

    let stats: Vec<ColumnStats> = (0..num_cols)
        .into_par_iter()
        .map(|col_idx| stats::compute_stats(&dataset.rows, col_idx))
        .collect::<Result<_, _>>()?;

    let missing: Vec<usize> = (0..num_cols)
        .into_par_iter()
        .map(|col_idx| {
            dataset
                .rows
                .iter()
                .filter(|row| row[col_idx].is_empty() || row[col_idx] == "NA")
                .count()
        })
        .collect();

    let duplicates = detect_duplicates(&dataset.rows);

    let outliers: Vec<Vec<usize>> = (0..num_cols)
        .into_par_iter()
        .map(|col_idx| detect_outliers(&dataset.rows, col_idx, &stats[col_idx]))
        .collect();

    let types: Vec<TypeInference> = (0..num_cols)
        .into_par_iter()
        .map(|col_idx| types::infer_type(&dataset.rows, col_idx))
        .collect();

    let dependency_scores = compute_dependency_scores(dataset, &stats)?;
    let drift_scores = detect_drift(dataset, &stats)?;
    let cardinality = compute_cardinality(dataset)?;
    let distributions = compute_distribution(dataset, &stats)?;
    let consistency_issues = check_consistency(dataset)?;
    let temporal_patterns = detect_temporal_patterns(dataset)?;
    let transform_suggestions = suggest_transformations(&stats)?;
    let noise_scores = estimate_noise(dataset, &stats)?;
    let redundancy_pairs = detect_redundancy(dataset)?;

    let total_rows = dataset.rows.len();
    let total_cells = total_rows * num_cols;
    let missing_pct = missing.iter().sum::<usize>() as f64 / total_cells as f64 * 100.0;
    let unique_rows: std::collections::HashSet<&Vec<String>> = dataset.rows.iter().collect();
    let unique_pct = unique_rows.len() as f64 / total_rows as f64 * 100.0;

    let top_values: Vec<(String, Vec<(String, usize)>)> = (0..num_cols)
        .into_par_iter()
        .map(|col_idx| {
            let mut counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for row in &dataset.rows {
                let val = &row[col_idx];
                if !val.is_empty() && val != "NA" {
                    *counts.entry(val.clone()).or_insert(0) += 1;
                }
            }
            let mut sorted: Vec<(String, usize)> = counts.into_iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(&a.1));
            (
                dataset.headers[col_idx].clone(),
                sorted.into_iter().take(5).collect(),
            )
        })
        .collect();

    let correlations: Vec<Vec<f64>> = (0..num_cols)
        .into_par_iter()
        .map(|i| {
            (0..num_cols)
                .map(|j| {
                    if i == j {
                        return 1.0;
                    }
                    let col_i: Vec<f64> = dataset
                        .rows
                        .iter()
                        .filter_map(|row| row[i].parse::<f64>().ok())
                        .collect();
                    let col_j: Vec<f64> = dataset
                        .rows
                        .iter()
                        .filter_map(|row| row[j].parse::<f64>().ok())
                        .collect();
                    if col_i.len() != col_j.len() || col_i.is_empty() {
                        return 0.0;
                    }
                    if let (Some(mean_i), Some(std_i)) = (stats[i].mean, stats[i].std_dev) {
                        if let (Some(mean_j), Some(std_j)) = (stats[j].mean, stats[j].std_dev) {
                            let cov = col_i
                                .iter()
                                .zip(col_j.iter())
                                .map(|(&x, &y)| (x - mean_i) * (y - mean_j))
                                .sum::<f64>()
                                / col_i.len() as f64;
                            cov / (std_i * std_j)
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                })
                .collect()
        })
        .collect();

    let target_idx = dataset
        .headers
        .iter()
        .position(|h| h.to_lowercase().contains("target"))
        .unwrap_or(0);
    let target_values: Vec<f64> = dataset
        .rows
        .iter()
        .filter_map(|row| row[target_idx].parse::<f64>().ok())
        .collect();
    let feature_importance: Vec<(usize, f64)> = (0..num_cols)
        .into_par_iter()
        .filter_map(|col_idx| {
            let col_values: Vec<f64> = dataset
                .rows
                .iter()
                .filter_map(|row| row[col_idx].parse::<f64>().ok())
                .collect();
            if col_idx != target_idx
                && !col_values.is_empty()
                && col_values.len() == target_values.len()
            {
                let corr = if let (Some(mean_i), Some(std_i)) =
                    (stats[col_idx].mean, stats[col_idx].std_dev)
                {
                    if let (Some(mean_t), Some(std_t)) =
                        (stats[target_idx].mean, stats[target_idx].std_dev)
                    {
                        let cov = col_values
                            .iter()
                            .zip(target_values.iter())
                            .map(|(&x, &y)| (x - mean_i) * (y - mean_t))
                            .sum::<f64>()
                            / col_values.len() as f64;
                        cov / (std_i * std_t)
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };
                Some((col_idx, corr.abs()))
            } else {
                None
            }
        })
        .collect();
    let mut feature_importance = feature_importance;
    feature_importance.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let anomalies: Vec<(usize, f64, usize)> = (0..num_cols)
        .into_par_iter()
        .flat_map(|col_idx| {
            let col_values: Vec<(f64, usize)> = dataset
                .rows
                .iter()
                .enumerate()
                .filter_map(|(idx, row)| row[col_idx].parse::<f64>().ok().map(|v| (v, idx)))
                .collect();
            if let (Some(mean), Some(std_dev)) = (stats[col_idx].mean, stats[col_idx].std_dev) {
                col_values
                    .into_iter()
                    .filter(|&(val, _)| (val - mean).abs() / std_dev > 3.0)
                    .map(move |(val, idx)| (col_idx, val, idx))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        })
        .collect();

    let description = Description::new(
        stats,
        missing,
        duplicates,
        outliers,
        types,
        dependency_scores,
        drift_scores,
        cardinality,
        distributions,
        consistency_issues,
        temporal_patterns,
        transform_suggestions,
        noise_scores,
        redundancy_pairs,
        total_rows,
        missing_pct,
        unique_pct,
        top_values,
        correlations,
        feature_importance,
        anomalies,
    );

    render_tui(dataset, &description)?;

    Ok(description)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_empty() {
        let dataset = Dataset::new(vec![], vec![]);
        assert!(matches!(describe(&dataset), Err(PrestoError::EmptyDataset)));
    }
}
