use pyo3::prelude::*;
use numpy::{PyArray1, IntoPyArray};

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
