use chrono::{Datelike, NaiveDateTime};
use influxdb::Timestamp::Seconds;

pub struct Timestamp {
    pub epoch_seconds: i64,
    pub year: i32,
    pub month: u32,
    pub month_string: String,
}

impl Timestamp {
    pub(crate) fn to_influxdb(&self) -> influxdb::Timestamp {
        return Seconds(self.epoch_seconds as u128)
    }
}

pub fn parse_timestamp(timestamp_string: &str) -> Result<Timestamp, &'static str> {
    let epoch_seconds: i64 = timestamp_string.parse().map_err(|_| "parse failed")?;
    println!("epoch_time {}", epoch_seconds);

    let naive_date_time = NaiveDateTime::from_timestamp_opt(epoch_seconds, 0).unwrap();
    println!("naive date time: {}", &naive_date_time);
    let year = naive_date_time.year();
    let month = naive_date_time.month();
    let month_string = format!("{:0>4}-{:0>2}", year, month);
    return Ok(Timestamp {
        epoch_seconds,
        year,
        month,
        month_string,
    });
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_parse_timestamp() -> Result<(), &'static str> {
        let datetime = Utc.with_ymd_and_hms(2023, 2, 15, 20, 15, 32).unwrap();
        let epoch_seconds = datetime.timestamp();
        let epoch_seconds_string = epoch_seconds.to_string();

        let result = parse_timestamp(&epoch_seconds_string)?;

        assert_eq!(result.epoch_seconds, 1676492132);
        assert_eq!(result.year, 2023);
        assert_eq!(result.month, 2);
        assert_eq!(result.month_string, "2023-02");
        Ok(())
    }
}