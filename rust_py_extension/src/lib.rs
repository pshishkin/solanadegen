use pyo3::prelude::*;
use numpy::{PyArray1, IntoPyArray};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use pyo3::exceptions::PyRuntimeError;  // Import PyRuntimeError
use std::collections::HashMap;  // Import HashMap
use chrono::{DateTime, Utc, TimeZone, LocalResult, FixedOffset, NaiveDateTime};


struct ParsedRecord {
    timestamp: DateTime<FixedOffset>,
    token_delta: f64,
    sol_delta: f64,
}

#[derive(Deserialize)]
struct Record {
    timestamp: String,
    mint: String,
    token_delta: f64,
    sol_delta: f64,
}

#[derive(Serialize)]
struct Feature {
    mint: String,
    timestamp: String,
    last_price: f64,
    token_delta: f64,
    sol_delta: f64,
    token_delta_1h: f64,
    sol_delta_1h: f64,
    token_delta_1d: f64,
    sol_delta_1d: f64,
    token_delta_1w: f64,
    sol_delta_1w: f64,
}

fn create_max_timestamp() -> DateTime<FixedOffset> {
    // Create a DateTime<Utc> from a timestamp
    let utc_timestamp = Utc.timestamp_opt(0, 0);

    // Handle the LocalResult, safely unwrap because 0 is always valid
    let utc_datetime = match utc_timestamp {
        LocalResult::Single(dt) => dt,
        _ => panic!("Invalid timestamp"),
    };

    // Convert the DateTime<Utc> to DateTime<FixedOffset> with a zero offset
    utc_datetime.with_timezone(&FixedOffset::east(0))
}

// function that truncates a timestamp to a given time quantization
fn truncate_timestamp(timestamp: DateTime<FixedOffset>, time_quantization_seconds: i32) -> DateTime<FixedOffset> {
    let timestamp_seconds = timestamp.timestamp();
    let truncated_timestamp_seconds = timestamp_seconds - (timestamp_seconds % time_quantization_seconds as i64);
    // Convert the truncated timestamp seconds to NaiveDateTime
    let naive_truncated_timestamp = NaiveDateTime::from_timestamp_opt(truncated_timestamp_seconds, 0)
    .expect("Failed to create NaiveDateTime from timestamp");
    // Create a DateTime with the desired offset (assuming you want to keep the original offset of the timestamp)
    // let truncated_timestamp = DateTime::<FixedOffset>::from_utc(naive_truncated_timestamp, timestamp.offset().clone());
    let truncated_timestamp = DateTime::from_naive_utc_and_offset(naive_truncated_timestamp, timestamp.offset().clone());
    truncated_timestamp
}

fn serialize_timestamp(timestamp: DateTime<FixedOffset>) -> String {
    let timestamp_str = timestamp.format("%Y-%m-%d %H:%M:%S").to_string();
    timestamp_str
}


#[pyfunction]
fn process_dataframe(_py: Python, csv_data: String, time_quantization_seconds: i32) -> PyResult<(String, String)> {
    let mut rdr = ReaderBuilder::new().from_reader(csv_data.as_bytes());
    let mut mints_map: HashMap<String, Vec<ParsedRecord>> = HashMap::new();
    // map with mint, datetime pair as key, features as values
    let mut keys_map: HashMap<(DateTime<FixedOffset>, String), bool> = HashMap::new();
    let mut min_timestamp: DateTime<FixedOffset> = Utc::now().into();
    //zero timestamp
    let mut max_timestamp: DateTime<FixedOffset> = create_max_timestamp();

    for result in rdr.deserialize() {
        let record: Record = match result {
            Ok(r) => r,
            Err(e) => {
                // Convert csv::Error to PyErr
                return Err(PyRuntimeError::new_err(format!("CSV error: {}", e)));
            },
        };

        let tz_timestamp_str = format!("{}+00:00", record.timestamp);

        let timestamp = match DateTime::parse_from_str(&tz_timestamp_str, "%Y-%m-%d %H:%M:%S%z") {
            Ok(ts) => ts,
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!("Timestamp parse error: {}, {}", e, record.timestamp)));
            },
        };

        let parsed_record = ParsedRecord {
            timestamp,
            token_delta: record.token_delta,
            sol_delta: record.sol_delta,
        };
        if timestamp < min_timestamp {
            min_timestamp = timestamp;
        }
        if timestamp > max_timestamp {
            max_timestamp = timestamp;
        }
        if mints_map.contains_key(&record.mint) {
            let parsed_records = mints_map.get_mut(&record.mint).unwrap();
            parsed_records.push(parsed_record);
        } else {
            mints_map.insert(record.mint.clone(), vec![parsed_record]);
        }
        let truncated_timestamp = truncate_timestamp(timestamp, time_quantization_seconds);
        keys_map.insert((truncated_timestamp, record.mint.clone()), true);
    }

    // Go over mints_map and sort each vector by timestamp
    for (_mint, parsed_records) in mints_map.iter_mut() {
        parsed_records.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }

    let features = generate_features(&mints_map);
    let csv_str = serialize_features_to_csv(features);
    // Return processed data or a confirmation message
    let result = format!("Records stored: {:?}, keys got: {:?}, from {:?} to {:?}", mints_map.len(), keys_map.len(), min_timestamp, max_timestamp);
    Ok((result, csv_str))
}


fn generate_features(mints_map: &HashMap<String, Vec<ParsedRecord>>) -> Vec<Feature> {
    let mut ans: Vec<Feature> = Vec::new();
    for (_mint, parsed_records) in mints_map.iter() {
        for parsed_record in parsed_records.iter() {
            let feature = Feature {
                mint: _mint.clone(),
                timestamp: serialize_timestamp(parsed_record.timestamp),
                last_price: 0.0,
                token_delta: parsed_record.token_delta,
                sol_delta: parsed_record.sol_delta,
                token_delta_1h: 0.0,
                sol_delta_1h: 0.0,
                token_delta_1d: 0.0,
                sol_delta_1d: 0.0,
                token_delta_1w: 0.0,
                sol_delta_1w: 0.0,
            };
            ans.push(feature);
        }
    }
    ans
}

fn serialize_features_to_csv(features: Vec<Feature>) -> String {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for feature in features.iter() {
        wtr.serialize(feature).expect("Failed to serialize feature");
    }
    let csv_str = String::from_utf8(wtr.into_inner().expect("Failed to get inner writer")).expect("Failed to convert csv bytes to string");
    csv_str
}


#[pyfunction]
fn multiply_by_two(py: Python, array: &PyArray1<f64>) -> PyResult<PyObject> {
    let array = unsafe {
        array.as_array().to_owned() * 2.0
    };
    let pyarray = array.into_pyarray(py).to_object(py);
    Ok(pyarray)
}


#[pymodule]
fn rust_py_extension(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(multiply_by_two, m)?)?;
    m.add_function(wrap_pyfunction!(process_dataframe, m)?)?;
    Ok(())
}


pub fn add(left: usize, right: usize) -> usize {
    left + right
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
