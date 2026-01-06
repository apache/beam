use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
mod word_processing {
    use pyo3::prelude::*;
    use regex::Regex;

    /// Builds the map of string to tuple(string, int).
    #[pyfunction]
    fn map_to_int(a: String) -> PyResult<(String, u32)> {
        Ok((a, 1))
    }

    /// Extracts individual words from a line of text.
    #[pyfunction]
    fn extract_words(a: String) -> PyResult<Vec<String>> {
        let re = Regex::new(r"[\w\']+").unwrap();
        Ok(re.find_iter(&a).map(|m| m.as_str().to_string()).collect())
    }
}
