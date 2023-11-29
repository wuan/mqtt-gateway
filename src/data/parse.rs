use chrono::{Datelike, DateTime};

pub fn parse_timestamp(timestamp_string: &str) -> Result<i64, &'static str> {
    Ok(DateTime::parse_from_rfc3339(timestamp_string).map_err(|_| "parse failed")?.timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp_with_timezone() -> Result<(), &'static str> {
        let result = parse_timestamp("2023-11-29T21:16:32.511722+00:00")?;

        assert_eq!(result, 1701292592);
        Ok(())
    }

    #[test]
    fn test_parse_timestamp() -> Result<(), &'static str> {
        let result = parse_timestamp("2023-11-29Z21:16:32.511722")?;

        assert_eq!(result, 1701292592);
        Ok(())
    }
}