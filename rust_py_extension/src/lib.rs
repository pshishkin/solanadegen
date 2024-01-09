use pyo3::prelude::*;
use numpy::{PyArray1, IntoPyArray};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use pyo3::exceptions::PyRuntimeError;  // Import PyRuntimeError
use std::collections::HashMap;  // Import HashMap
use chrono::{DateTime, Utc, TimeZone, LocalResult, FixedOffset, NaiveDateTime};
use std::error::Error;


#[derive(Clone)]
struct ParsedRecord {
    timestamp: DateTime<FixedOffset>,
    truncated_timestamp: i64,
    mint: String,
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

const FEATURES_LENGTH: usize = 65;

const FEATURE_NAMES: [&str; FEATURES_LENGTH] = [
    "found_points_now", "price_sma_now", "price_ema_now", "price_rsi_now", "found_points_10m", "price_sma_10m", "price_ema_10m", "price_rsi_10m", "found_points_1h", "price_sma_1h", "price_ema_1h", "price_rsi_1h", "found_points_4h", "price_sma_4h", "price_ema_4h", "price_rsi_4h", "found_points_1d", "price_sma_1d", "price_ema_1d", "price_rsi_1d", "trcnt_sma_buy_now", "vol_sma_buy_now", "vol_ema_buy_now", "trcnt_sma_buy_10m", "vol_sma_buy_10m", "vol_ema_buy_10m", "trcnt_sma_buy_1h", "vol_sma_buy_1h", "vol_ema_buy_1h", "trcnt_sma_buy_4h", "vol_sma_buy_4h", "vol_ema_buy_4h", "trcnt_sma_buy_1d", "vol_sma_buy_1d", "vol_ema_buy_1d", "trcnt_sma_sell_now", "vol_sma_sell_now", "vol_ema_sell_now", "trcnt_sma_sell_10m", "vol_sma_sell_10m", "vol_ema_sell_10m", "trcnt_sma_sell_1h", "vol_sma_sell_1h", "vol_ema_sell_1h", "trcnt_sma_sell_4h", "vol_sma_sell_4h", "vol_ema_sell_4h", "trcnt_sma_sell_1d", "vol_sma_sell_1d", "vol_ema_sell_1d", "trcnt_sma_sum_now", "vol_sma_sum_now", "vol_ema_sum_now", "trcnt_sma_sum_10m", "vol_sma_sum_10m", "vol_ema_sum_10m", "trcnt_sma_sum_1h", "vol_sma_sum_1h", "vol_ema_sum_1h", "trcnt_sma_sum_4h", "vol_sma_sum_4h", "vol_ema_sum_4h", "trcnt_sma_sum_1d", "vol_sma_sum_1d", "vol_ema_sum_1d"
];

struct AggregationKind {
    name: &'static str,
    quant: i64,
    points: usize,
}

const AGGREGATION_KINDS: [AggregationKind; 5] = [
    AggregationKind { name: "now", quant: -60 * 10, points: 1 },
    AggregationKind { name: "10m", quant: -60 * 10, points: 14 },
    AggregationKind { name: "1h", quant: -60 * 60, points: 14 },
    AggregationKind { name: "4h", quant: -4 * 60 * 60, points: 14 },
    AggregationKind { name: "1d", quant: -24 * 60 * 60, points: 14 },
];

struct Features {
    features1: Features1,
    features2: Features2,
}

struct Features2 {
    features: [f64; FEATURES_LENGTH],
}

#[derive(Serialize)]
struct Features1 {
    mint: String,
    timestamp: String,
    quant_price: f64,
    buy_price: f64,
    sell_price: f64,
}

struct FeaturesDF {
    name_to_index: HashMap<String, usize>,
    features: Vec<Features>,
}

fn new_features_df() -> FeaturesDF {
    let mut table = FeaturesDF {
        name_to_index: HashMap::new(),
        features: Vec::new(),
    };
    for (i, name) in FEATURE_NAMES.iter().enumerate() {
        table.name_to_index.insert(name.to_string(), i);
    }
    table
}

fn assign_feature_last_row(table: &mut FeaturesDF, name: String, value: f64) {
    let index = table.name_to_index.get(&name).expect(&format!("Name '{}' not found in name_to_index", name));
    let last_row = table.features.last_mut().unwrap();
    last_row.features2.features[*index] = value;
}

struct Agregates {
    min_global_timestamp: i64,
    max_global_timestamp: i64,
    len: usize,
    quant_seconds: i64,
    tables: HashMap<String, Table>,
}

struct Table {
    min_timestamp: i64,
    max_timestamp: i64,
    len: usize,
    prices: Vec<f64>,
    buy_volumes: Vec<f64>,
    sell_volumes: Vec<f64>,
    buy_counts: Vec<f64>,
    sell_counts: Vec<f64>,
}

enum TableType {
    Prices,
    BuyVolumes,
    SellVolumes,
    BuyCounts,
    SellCounts,
}

fn get_typed_table(table_type: TableType, table: &Table) -> &Vec<f64> {
    match table_type {
        TableType::Prices => &table.prices,
        TableType::BuyVolumes => &table.buy_volumes,
        TableType::SellVolumes => &table.sell_volumes,
        TableType::BuyCounts => &table.buy_counts,
        TableType::SellCounts => &table.sell_counts,
    }
}

fn get_table_reverse_cumsum(agregates: &Agregates, mint: &String, table_type: TableType, timestamp: i64, quant_seconds: i64, points: usize) -> Vec<f64> {
    assert!(matches!(table_type, TableType::BuyVolumes | TableType::SellVolumes | TableType::BuyCounts | TableType::SellCounts));
    let mut cumsums = get_table_subvec(agregates, mint, table_type, timestamp, quant_seconds, points + 1);
    if cumsums.len() < points + 1 {
        cumsums.insert(0, 0.0);
    }
    // now reverse cumsum operation
    let mut ans = Vec::new();
    for i in 0..(cumsums.len() - 1) {
        ans.push(cumsums[i + 1] - cumsums[i]);
    }
    ans
}

fn get_table_subvec(agregates: &Agregates, mint: &String, table_type: TableType, timestamp: i64, quant_seconds: i64, points: usize) -> Vec<f64> {
    let mut ans = Vec::new();
    let mut timestamp = timestamp;
    let table = agregates.tables.get(mint).unwrap();
    let typed_table = get_typed_table(table_type, table);
    assert_eq!((timestamp - table.min_timestamp) % agregates.quant_seconds, 0);
    assert_eq!(quant_seconds % agregates.quant_seconds, 0);
    // check that timestamp is in range of global timestamps. return -1 if not
    if timestamp < agregates.min_global_timestamp || timestamp > agregates.max_global_timestamp {
        return ans;
    }
    // check if timestamp is in range of table timestamps. return 0 if not
    if timestamp < table.min_timestamp || timestamp > table.max_timestamp {
        return ans;
    }

    while ans.len() < points {
        let table_index = ((timestamp - table.min_timestamp) / agregates.quant_seconds) as usize;
        let val = typed_table[table_index];
        ans.push(val);
        timestamp += quant_seconds;
        if timestamp < table.min_timestamp || timestamp > table.max_timestamp {
            break;
        }
    }
    if quant_seconds < 0 {
        ans.reverse();
    }
    ans
}

fn add_vecs(a: &Vec<f64>, b: &Vec<f64>) -> Vec<f64> {
    assert_eq!(a.len(), b.len());
    let mut ans = Vec::new();
    for i in 0..a.len() {
        ans.push(a[i] + b[i]);
    }
    ans
}

fn get_prices_vec(agregates: &Agregates, mint: &String, timestamp: i64, quant_seconds: i64, points: usize) -> Vec<f64> {
    get_table_subvec(agregates, mint, TableType::Prices, timestamp, quant_seconds, points)
}

fn get_price(agregates: &Agregates, mint: &String, timestamp:i64) -> f64 {
    let prices = get_table_subvec(agregates, mint, TableType::Prices, timestamp, agregates.quant_seconds, 1);
    if prices.len() == 0 {
        return 0.0;
    }
    prices[0]
}

fn sma(values: &Vec<f64>) -> f64 {
    let mut sum = 0.0;
    for val in values.iter() {
        sum += val;
    }
    sum / std::cmp::max(1, values.len()) as f64
}

fn ema(values: &Vec<f64>) -> f64 {
    if values.len() == 0 {
        return 0.0;
    }
    let k = 2.0 / (values.len() as f64 + 1.0);
    let mut ans = values[0];
    for i in 1..values.len() {
        ans = k * values[i] + (1.0 - k) * ans;
    }
    ans
}

fn relative_strength_index(values: &Vec<f64>) -> f64 {
    if values.len() == 0 {
        return 0.0;
    }
    let mut gains = Vec::new();
    let mut losses = Vec::new();
    for i in 1..values.len() {
        let delta = values[i] - values[i - 1];
        if delta > 0.0 {
            gains.push(delta);
        } 
        if delta < 0.0 {
            losses.push(delta.abs());
        }
    }
    let avg_gain = sma(&gains);
    let avg_loss = sma(&losses);
    if avg_loss == 0.0 {
        return 100.0;
    }
    100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
}


fn generate_features_row(
    agregates: &Agregates,
    features_df: &mut FeaturesDF,
    mint: &String,
    timestamp: i64,
) {
    // iterate over AGGREGATION_KINDS
    for a in AGGREGATION_KINDS {
        // JUST PRICES
        let prices = get_table_subvec(agregates, mint, TableType::Prices, timestamp, a.quant, a.points);
        assign_feature_last_row(features_df, format!("found_points_{}", a.name), prices.len() as f64);
        if prices.len() == 0 {
            continue;
        }
        let base_price = prices[prices.len() - 1];
        if base_price == 0.0 {
            continue;
        }
        assign_feature_last_row(features_df, format!("price_sma_{}", a.name), sma(&prices) / base_price);
        assign_feature_last_row(features_df, format!("price_ema_{}", a.name), ema(&prices) / base_price);
        // rsi
        assign_feature_last_row(features_df, format!("price_rsi_{}", a.name), relative_strength_index(&prices));
        // volumes
        let buy_volumes = get_table_reverse_cumsum(agregates, mint, TableType::BuyVolumes, timestamp, a.quant, a.points);
        let sell_volumes = get_table_reverse_cumsum(agregates, mint, TableType::SellVolumes, timestamp, a.quant, a.points);
        let sum_volumes = add_vecs(&buy_volumes, &sell_volumes);
        assign_feature_last_row(features_df, format!("vol_sma_buy_{}", a.name), sma(&buy_volumes));
        assign_feature_last_row(features_df, format!("vol_ema_buy_{}", a.name), ema(&buy_volumes));
        assign_feature_last_row(features_df, format!("vol_sma_sell_{}", a.name), sma(&sell_volumes));
        assign_feature_last_row(features_df, format!("vol_ema_sell_{}", a.name), ema(&sell_volumes));
        assign_feature_last_row(features_df, format!("vol_sma_sum_{}", a.name), sma(&sum_volumes));
        assign_feature_last_row(features_df, format!("vol_ema_sum_{}", a.name), ema(&sum_volumes));
        // counts
        let buy_counts = get_table_reverse_cumsum(agregates, mint, TableType::BuyCounts, timestamp, a.quant, a.points);
        let sell_counts = get_table_reverse_cumsum(agregates, mint, TableType::SellCounts, timestamp, a.quant, a.points);
        let sum_counts = add_vecs(&buy_counts, &sell_counts);
        assign_feature_last_row(features_df, format!("trcnt_sma_buy_{}", a.name), sma(&buy_counts));
        assign_feature_last_row(features_df, format!("trcnt_sma_sell_{}", a.name), sma(&sell_counts));
        assign_feature_last_row(features_df, format!("trcnt_sma_sum_{}", a.name), sma(&sum_counts));
    }
}

fn create_datetime_from_timestamp(timestamp: i64) -> DateTime<FixedOffset> {
    // Create a DateTime<Utc> from a timestamp
    let utc_timestamp = Utc.timestamp_opt(timestamp, 0);

    // Handle the LocalResult, safely unwrap because 0 is always valid
    let utc_datetime = match utc_timestamp {
        LocalResult::Single(dt) => dt,
        _ => panic!("Invalid timestamp"),
    };

    // Convert the DateTime<Utc> to DateTime<FixedOffset> with a zero offset
    utc_datetime.with_timezone(&FixedOffset::east(0))
}

// function that truncates a timestamp to a given time quantization
fn truncate_timestamp(timestamp: DateTime<FixedOffset>, time_quantization_seconds: i64) -> DateTime<FixedOffset> {
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

fn build_table(records: &Vec<ParsedRecord>, time_quantization_seconds: i64) -> Table {
    let min_timestamp = truncate_timestamp(records[0].timestamp, time_quantization_seconds).timestamp();
    let max_timestamp = truncate_timestamp(records[records.len() - 1].timestamp, time_quantization_seconds).timestamp();
    let quantity = ((max_timestamp - min_timestamp) / time_quantization_seconds + 1) as usize;
    assert_eq!((max_timestamp - min_timestamp) % time_quantization_seconds, 0);
    // array
    let mut prices: Vec<f64> = vec![0.0; quantity];
    let mut buy_volumes: Vec<f64> = vec![0.0; quantity];
    let mut sell_volumes: Vec<f64> = vec![0.0; quantity];
    let mut buy_counts: Vec<f64> = vec![0.0; quantity];
    let mut sell_counts: Vec<f64> = vec![0.0; quantity];

    let mut j = 0;
    let mut till_timestamp = min_timestamp;
    let mut last_price = 0.0;
    let mut buy_volume_cumsum = 0.0;
    let mut sell_volume_cumsum = 0.0;
    let mut buy_count_cumsum = 0.0;
    let mut sell_count_cumsum = 0.0;
    for i in 0..quantity {
        till_timestamp = till_timestamp + time_quantization_seconds;
        while j < records.len() && records[j].timestamp.timestamp() < till_timestamp {
            last_price = (records[j].sol_delta / records[j].token_delta).abs();
            if records[j].sol_delta < 0.0 {
                buy_volume_cumsum += records[j].sol_delta.abs();
                buy_count_cumsum += 1.0;
            } 
            if records[j].sol_delta > 0.0 {
                sell_volume_cumsum += records[j].sol_delta.abs();
                sell_count_cumsum += 1.0;
            }
            j += 1;
        }
        prices[i] = last_price;
        buy_volumes[i] = buy_volume_cumsum;
        sell_volumes[i] = sell_volume_cumsum;
        buy_counts[i] = buy_count_cumsum;
        sell_counts[i] = sell_count_cumsum;
    }
    let table = Table {
        min_timestamp,
        max_timestamp,
        len: quantity,
        prices,
        buy_volumes,
        sell_volumes,
        buy_counts,
        sell_counts,
    };
    table
}

// fn serialize_timestamp(timestamp: DateTime<FixedOffset>) -> String {
fn serialize_timestamp(timestamp: i64) -> String {
    // convert to datetime first
    let datetime = create_datetime_from_timestamp(timestamp);
    let timestamp_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
    timestamp_str
}

#[pyfunction]
fn process_dataframe(
    _py: Python, 
    csv_data: String,
    time_quantization_seconds: i64,
    buy_seconds: i64,
    sell_seconds_from: i64,
    sell_seconds_to: i64,
    sell_seconds_step: i64,
) -> PyResult<(String, String)> {
    let mut rdr = ReaderBuilder::new().from_reader(csv_data.as_bytes());
    let mut mints_map: HashMap<String, Vec<ParsedRecord>> = HashMap::new();
    // map with mint, datetime pair as key, features as values
    let mut keys_map: HashMap<(i64, String), bool> = HashMap::new();
    // max i64
    let mut min_timestamp: i64 = i64::MAX;
    //zero timestamp
    let mut max_timestamp: i64 = 0;
    let mut local_parsed_records: Vec<ParsedRecord> = Vec::new();

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

        let truncated_timestamp = truncate_timestamp(timestamp, time_quantization_seconds).timestamp();
        let parsed_record = ParsedRecord {
            timestamp,
            truncated_timestamp,
            mint: record.mint,
            token_delta: record.token_delta,
            sol_delta: record.sol_delta,
        };
        
        if truncated_timestamp < min_timestamp {
            min_timestamp = truncated_timestamp;
        }
        if truncated_timestamp > max_timestamp {
            max_timestamp = truncated_timestamp;
        }
        local_parsed_records.push(parsed_record);
    }

    min_timestamp += time_quantization_seconds;
    max_timestamp -= time_quantization_seconds;

    for parsed_record in local_parsed_records.iter() {
        if parsed_record.truncated_timestamp < min_timestamp || parsed_record.truncated_timestamp > max_timestamp {
            continue;
        }
        if mints_map.contains_key(&parsed_record.mint) {
            let parsed_records = mints_map.get_mut(&parsed_record.mint).unwrap();
            parsed_records.push(parsed_record.clone());
        } else {
            mints_map.insert(parsed_record.mint.clone(), vec![parsed_record.clone()]);
        }

        keys_map.insert((parsed_record.truncated_timestamp, parsed_record.mint.clone()), true);
    }

    let mut agregates = Agregates {
        min_global_timestamp: min_timestamp,
        max_global_timestamp: max_timestamp,
        len: (max_timestamp - min_timestamp) as usize / (time_quantization_seconds as usize) + 1,
        quant_seconds: time_quantization_seconds,
        tables: HashMap::new(),
    };
    // Go over mints_map and sort each vector by timestamp
    for (_mint, parsed_records) in mints_map.iter_mut() {
        parsed_records.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        let table = build_table(parsed_records, time_quantization_seconds);
        agregates.tables.insert(_mint.clone(), table);
    }

    let features = generate_features(&keys_map, &agregates, buy_seconds, sell_seconds_from, sell_seconds_to, sell_seconds_step);
    let csv_str = match serialize_features_to_csv(features) {
        Ok(s) => s,
        Err(e) => {
            return Err(PyRuntimeError::new_err(format!("CSV serialize error: {}", e)));
        },
    };
    // Return processed data or a confirmation message
    let result = format!("Records stored: {:?}, keys got: {:?}, from {:?} to {:?}", mints_map.len(), keys_map.len(), min_timestamp, max_timestamp);
    Ok((result, csv_str))
}


fn generate_features(
    keys_map: &HashMap<(i64, String), bool>, 
    agregates: &Agregates,
    buy_seconds: i64,
    sell_seconds_from: i64,
    sell_seconds_to: i64,
    sell_seconds_step: i64,
) -> FeaturesDF {
    // get keys, sort them
    let mut keys: Vec<(i64, String)> = keys_map.keys().cloned().collect();
    keys.sort_by(|a, b| a.cmp(&b));
    let mut features_df = new_features_df();
    for (q_timestamp, mint) in keys.iter() {

        let mut buy_price = get_price(agregates, mint, q_timestamp + buy_seconds);

        let mut sell_price = 0.;
        let sell_points = ((sell_seconds_to - sell_seconds_from) / sell_seconds_step + 1) as usize;
        let mut sell_prices_vec = get_prices_vec(
            agregates, 
            mint, 
            q_timestamp + sell_seconds_from,
            sell_seconds_step, 
            sell_points,
        );
        // if sell_seconds_to is inside the global aggregates range and sell_prices_vec is not long enough, append the last price
        if sell_seconds_to + q_timestamp <= agregates.max_global_timestamp && sell_prices_vec.len() < sell_points {
            let mut cur_price = buy_price;
            if sell_prices_vec.len() > 0 {
                cur_price = sell_prices_vec[sell_prices_vec.len() - 1];
            }
            while sell_prices_vec.len() < sell_points {
                sell_prices_vec.push(cur_price);
            }
        }
        if sell_prices_vec.len() == sell_points {
            // avg of prices
            sell_price = sell_prices_vec.iter().sum::<f64>() / sell_prices_vec.len() as f64;
        }

        let features_row = Features {
            features1: Features1 {
                mint: mint.clone(),
                timestamp: serialize_timestamp(q_timestamp.clone()),
                quant_price: get_price(agregates, mint, q_timestamp.clone()),
                buy_price: buy_price,
                sell_price: sell_price,
            },
            features2: Features2 {
                features: [-1.; FEATURES_LENGTH],
            },
        };
        features_df.features.push(features_row);
        
        generate_features_row(agregates, &mut features_df, mint, q_timestamp.clone());

    }
    features_df
}

fn serialize_features_to_csv(features: FeaturesDF) -> Result<String, Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(vec![]);

    // Write the header row manually, combining Features1 and Features2 field names
    let mut header_row = Vec::new();
    for &name in ["mint", "timestamp", "quant_price", "buy_price", "sell_price"].iter() {
        header_row.push(name.to_string());
    }
    for &name in FEATURE_NAMES.iter() {
        header_row.push(name.to_string());
    }
    wtr.write_record(&header_row)?;

    for feature in features.features.iter() {
        let mut record = Vec::new();
        
        // Serialize Features1
        let features1 = serde_json::to_value(&feature.features1)?;
        if let Some(map) = features1.as_object() {
            for key in ["mint", "timestamp", "quant_price", "buy_price", "sell_price"].iter() {
                let str = map.get(*key).unwrap_or(&serde_json::Value::Null).to_string().replace("\"", "");
                record.push(str);
            }
        }

        // Add Features2 values
        for &val in feature.features2.features.iter() {
            record.push(val.to_string());
        }

        // Write the combined record
        wtr.write_record(&record)?;
    }

    let csv_str = String::from_utf8(wtr.into_inner()?)?;
    Ok(csv_str)
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
