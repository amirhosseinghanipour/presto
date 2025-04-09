#[derive(Debug, serde::Serialize)]
pub enum TypeInference {
    Integer,
    Float,
    String,
    Boolean,
    Mixed,
}

pub fn infer_type(rows: &[Vec<String>], col_idx: usize) -> TypeInference {
    let mut is_int = true;
    let mut is_float = true;
    let mut is_bool = true;

    for row in rows {
        let val = &row[col_idx];
        if val.is_empty() || val == "NA" {
            continue;
        }
        if is_int && val.parse::<i64>().is_err() {
            is_int = false;
        }
        if is_float && val.parse::<f64>().is_err() {
            is_float = false;
        }
        if is_bool && !matches!(val.to_lowercase().as_str(), "true" | "false" | "1" | "0") {
            is_bool = false;
        }
        if !is_int && !is_float && !is_bool {
            return TypeInference::String;
        }
    }

    if is_int && !is_float && !is_bool {
        TypeInference::Integer
    } else if is_float && !is_bool {
        TypeInference::Float
    } else if is_bool {
        TypeInference::Boolean
    } else {
        TypeInference::Mixed
    }
}
