use crate::{Dataset, PrestoError};
use rayon::prelude::*;
use std::collections::HashSet;

pub fn detect_duplicates(rows: &[Vec<String>]) -> usize {
    let unique: HashSet<&Vec<String>> = rows.par_iter().collect();
    rows.len() - unique.len()
}

pub fn detect_outliers(
    rows: &[Vec<String>],
    col_idx: usize,
    stats: &crate::stats::ColumnStats,
) -> Vec<usize> {
    if stats.mean.is_none() || stats.std_dev.is_none() {
        return vec![];
    }
    let mean = stats.mean.unwrap();
    let std_dev = stats.std_dev.unwrap();
    let z_threshold = 3.0;

    rows.par_iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            if row[col_idx].is_empty() || row[col_idx] == "NA" {
                None
            } else if let Ok(val) = row[col_idx].parse::<f64>() {
                let z_score = (val - mean).abs() / std_dev;
                if z_score > z_threshold {
                    Some(idx)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

pub fn check_consistency(dataset: &Dataset) -> Result<Vec<usize>, PrestoError> {
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
            let issues = values
                .iter()
                .filter(|&&v| {
                    if let Ok(num) = v.parse::<f64>() {
                        let header = dataset.headers[col_idx].to_lowercase();
                        if header.contains("age")
                            || header.contains("count")
                            || header.contains("size")
                        {
                            num < 0.0
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .count();
            Ok(issues)
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn detect_redundancy(dataset: &Dataset) -> Result<Vec<(usize, usize, f64)>, PrestoError> {
    let num_cols = dataset.headers.len();
    let mut pairs = Vec::new();
    for i in 0..num_cols {
        let col_i: Vec<&str> = dataset.rows.iter().map(|row| row[i].as_str()).collect();
        for j in (i + 1)..num_cols {
            let col_j: Vec<&str> = dataset.rows.iter().map(|row| row[j].as_str()).collect();
            let matches = col_i
                .iter()
                .zip(col_j.iter())
                .filter(|&(&a, &b)| a == b && !a.is_empty() && a != "NA")
                .count();
            let total_valid = col_i
                .iter()
                .filter(|&&v| !v.is_empty() && v != "NA")
                .count();
            let similarity = if total_valid > 0 {
                matches as f64 / total_valid as f64
            } else {
                0.0
            };
            if similarity > 0.9 {
                pairs.push((i, j, similarity));
            }
        }
    }
    Ok(pairs)
}
