use crate::{Dataset, PrestoError};
use rand::seq::SliceRandom;
use rayon::prelude::*;
use statrs::statistics::{Data, Distribution};
use std::str::FromStr;

#[derive(Debug, serde::Serialize)]
pub struct ColumnStats {
    pub mean: Option<f64>,
    pub median: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub std_dev: Option<f64>,
    pub variance: Option<f64>,
    pub skewness: Option<f64>,
    pub kurtosis: Option<f64>,
}

pub fn compute_stats(rows: &[Vec<String>], col_idx: usize) -> Result<ColumnStats, PrestoError> {
    let values: Vec<f64> = rows
        .par_iter()
        .filter_map(|row| {
            if row[col_idx].is_empty() || row[col_idx] == "NA" {
                None
            } else {
                f64::from_str(&row[col_idx]).ok()
            }
        })
        .collect();

    if values.is_empty() {
        return Ok(ColumnStats {
            mean: None,
            median: None,
            min: None,
            max: None,
            std_dev: None,
            variance: None,
            skewness: None,
            kurtosis: None,
        });
    }

    let data = Data::new(values.clone());
    let mean = Some(data.mean().unwrap());
    let mut sorted = values.clone();
    sorted.par_sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let median = Some(if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    });
    let min = Some(*sorted.first().unwrap());
    let max = Some(*sorted.last().unwrap());
    let std_dev = Some(data.std_dev().unwrap_or(0.0));
    let n = values.len() as f64;
    let mean_val = mean.unwrap();
    let std_dev_val = std_dev.unwrap();
    let variance = std_dev.map(|s| s.powi(2));
    let skewness = if std_dev_val > 0.0 {
        let skew_sum: f64 = values
            .par_iter()
            .map(|x| ((x - mean_val) / std_dev_val).powi(3))
            .sum();
        Some(skew_sum / n)
    } else {
        None
    };
    let kurtosis = if std_dev_val > 0.0 {
        let kurt_sum: f64 = values
            .par_iter()
            .map(|x| ((x - mean_val) / std_dev_val).powi(4))
            .sum();
        Some(kurt_sum / n - 3.0)
    } else {
        None
    };

    Ok(ColumnStats {
        mean,
        median,
        min,
        max,
        std_dev,
        variance,
        skewness,
        kurtosis,
    })
}

pub fn compute_dependency_scores(
    dataset: &Dataset,
    stats: &[ColumnStats],
) -> Result<Vec<f64>, PrestoError> {
    let num_cols = dataset.headers.len();
    let mut scores = vec![0.0; num_cols];

    for i in 0..num_cols {
        let mut total_impact = 0.0;
        let col_values: Vec<f64> = dataset
            .rows
            .par_iter()
            .filter_map(|row| row[i].parse::<f64>().ok())
            .collect();

        if col_values.is_empty() {
            continue;
        }

        for j in 0..num_cols {
            if i == j {
                continue;
            }
            let other_values: Vec<f64> = dataset
                .rows
                .par_iter()
                .filter_map(|row| row[j].parse::<f64>().ok())
                .collect();

            if other_values.len() != col_values.len() {
                continue;
            }

            let corr = if let (Some(mean_i), Some(std_i)) = (stats[i].mean, stats[i].std_dev) {
                if let (Some(mean_j), Some(std_j)) = (stats[j].mean, stats[j].std_dev) {
                    let cov = col_values
                        .iter()
                        .zip(other_values.iter())
                        .map(|(x, y)| (x - mean_i) * (y - mean_j))
                        .sum::<f64>()
                        / col_values.len() as f64;
                    cov / (std_i * std_j)
                } else {
                    0.0
                }
            } else {
                0.0
            };
            total_impact += corr.abs();
        }
        scores[i] = total_impact / (num_cols as f64 - 1.0);
    }
    Ok(scores)
}

pub fn detect_drift(dataset: &Dataset, stats: &[ColumnStats]) -> Result<Vec<f64>, PrestoError> {
    let num_cols = dataset.headers.len();
    let mid = dataset.rows.len() / 2;
    let mut drift_scores = vec![0.0; num_cols];

    for col_idx in 0..num_cols {
        let first_half: Vec<f64> = dataset.rows[..mid]
            .par_iter()
            .filter_map(|row| row[col_idx].parse::<f64>().ok())
            .collect();
        let second_half: Vec<f64> = dataset.rows[mid..]
            .par_iter()
            .filter_map(|row| row[col_idx].parse::<f64>().ok())
            .collect();

        if first_half.is_empty() || second_half.is_empty() {
            continue;
        }

        let first_data = Data::new(first_half.clone());
        let second_data = Data::new(second_half.clone());
        let first_mean = first_data.mean().unwrap_or(0.0);
        let second_mean = second_data.mean().unwrap_or(0.0);
        let drift = (first_mean - second_mean).abs() / stats[col_idx].std_dev.unwrap_or(1.0);
        drift_scores[col_idx] = drift;
    }
    Ok(drift_scores)
}

pub fn compute_cardinality(dataset: &Dataset) -> Result<Vec<usize>, PrestoError> {
    let num_cols = dataset.headers.len();
    (0..num_cols)
        .into_par_iter()
        .map(|col_idx| {
            let unique: std::collections::HashSet<&String> =
                dataset.rows.iter().map(|row| &row[col_idx]).collect();
            Ok(unique.len())
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn compute_distribution(
    dataset: &Dataset,
    stats: &[ColumnStats],
) -> Result<Vec<Vec<(f64, usize)>>, PrestoError> {
    let num_cols = dataset.headers.len();
    (0..num_cols)
        .into_par_iter()
        .map(|col_idx| {
            let values: Vec<f64> = dataset
                .rows
                .par_iter()
                .filter_map(|row| row[col_idx].parse::<f64>().ok())
                .collect();
            if values.is_empty() {
                return Ok(vec![]);
            }
            let min = stats[col_idx].min.unwrap_or(0.0);
            let max = stats[col_idx].max.unwrap_or(0.0);
            if min == max {
                return Ok(vec![(min, values.len())]);
            }
            let bin_size = (max - min) / 10.0;
            let mut bins = vec![0; 10];
            for val in values {
                let bin = ((val - min) / bin_size).floor() as usize;
                let bin = bin.min(9);
                bins[bin] += 1;
            }
            Ok(bins
                .into_iter()
                .enumerate()
                .map(|(i, count)| (min + (i as f64 + 0.5) * bin_size, count))
                .collect())
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn detect_temporal_patterns(dataset: &Dataset) -> Result<Vec<String>, PrestoError> {
    use chrono::NaiveDateTime;
    let num_cols = dataset.headers.len();
    (0..num_cols)
        .into_par_iter()
        .map(|col_idx| {
            let values: Vec<&str> = dataset
                .rows
                .iter()
                .map(|row| row[col_idx].as_str())
                .filter(|&v| !v.is_empty() && v != "NA")
                .collect();
            if values.is_empty() {
                return Ok("None".to_string());
            }
            let is_date = values.iter().all(|&v| {
                NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S").is_ok()
                    || NaiveDateTime::parse_from_str(v, "%Y-%m-%d").is_ok()
            });
            if is_date {
                return Ok("Date-like".to_string());
            }
            if let Ok(nums) = values
                .iter()
                .map(|v| v.parse::<f64>())
                .collect::<Result<Vec<_>, _>>()
            {
                let increasing = nums.windows(2).all(|w| w[0] <= w[1]);
                let decreasing = nums.windows(2).all(|w| w[0] >= w[1]);
                if increasing && !decreasing {
                    return Ok("Increasing".to_string());
                } else if decreasing && !increasing {
                    return Ok("Decreasing".to_string());
                }
            }
            Ok("None".to_string())
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn suggest_transformations(stats: &[ColumnStats]) -> Result<Vec<String>, PrestoError> {
    stats
        .par_iter()
        .map(|stat| {
            if stat.mean.is_none() {
                return Ok("None".to_string());
            }
            let mut suggestions = Vec::new();
            if let Some(skew) = stat.skewness {
                if skew.abs() > 1.0 {
                    suggestions.push("Log transform (skewed)");
                }
            }
            if let Some(min) = stat.min {
                if min < 0.0 {
                    suggestions.push("Shift positive");
                }
            }
            if let (Some(min), Some(max)) = (stat.min, stat.max) {
                if max - min > 100.0 {
                    suggestions.push("Normalize");
                }
            }
            Ok(if suggestions.is_empty() {
                "None".to_string()
            } else {
                suggestions.join(", ")
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn estimate_noise(dataset: &Dataset, stats: &[ColumnStats]) -> Result<Vec<f64>, PrestoError> {
    let num_cols = dataset.headers.len();
    (0..num_cols)
        .into_par_iter()
        .map(|col_idx| {
            let mut rng = rand::thread_rng();
            let values: Vec<f64> = dataset
                .rows
                .par_iter()
                .filter_map(|row| row[col_idx].parse::<f64>().ok())
                .collect();
            if values.len() < 10 {
                return Ok(0.0);
            }
            let mut shuffled = values.clone();
            shuffled.shuffle(&mut rng);
            let mid = shuffled.len() / 2;
            let first_half = Data::new(shuffled[..mid].to_vec());
            let second_half = Data::new(shuffled[mid..].to_vec());
            let var1 = first_half.variance().unwrap_or(0.0);
            let var2 = second_half.variance().unwrap_or(0.0);
            let overall_var = stats[col_idx].std_dev.map(|s| s.powi(2)).unwrap_or(0.0);
            let noise = if overall_var > 0.0 {
                (var1 - var2).abs() / overall_var
            } else {
                0.0
            };
            Ok(noise.min(1.0))
        })
        .collect::<Result<Vec<_>, _>>()
}
