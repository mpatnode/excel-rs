mod postgres;
mod utils;

use std::io::Cursor;

use chrono::NaiveDateTime;
use excel_rs_csv::{bytes_to_csv, get_headers, get_next_record};
use excel_rs_xlsx::WorkBook;
use numpy::PyReadonlyArray2;
use postgres::PyPostgresClient;
use utils::chrono_to_xlsx_date;
use pyo3::{prelude::*, types::{PyBytes, PyList}};
use excel_rs_xlsx::typed_sheet::{TYPE_STRING, TYPE_NUMBER, TYPE_DATE};

#[pymodule]
fn _excel_rs<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    #[pyo3(name = "csv_to_xlsx")]
    fn csv_to_xlsx<'py>(py: Python<'py>, buf: Bound<'py, PyBytes>) -> Bound<'py, PyBytes> {
        let x = buf.as_bytes();

        let output_buffer = vec![];
        let mut workbook = WorkBook::new(Cursor::new(output_buffer));
        let mut worksheet = workbook.get_typed_worksheet(String::from("Sheet 1"));

        let mut reader = bytes_to_csv(x);
        let headers = get_headers(&mut reader);

        if let Some(headers) = headers {
            let headers_to_bytes = headers.iter().to_owned().collect();
            let header_types = vec![TYPE_STRING; headers.len()];
            if let Err(e) = worksheet.write_row(headers_to_bytes, &header_types) {
                panic!("{e}");
            }
        }

        if let Some(record) = get_next_record(&mut reader) {
            let row_data: Vec<&[u8]> = record.iter().to_owned().collect();
            let types = worksheet.infer_row_types(&row_data);
            if let Err(e) = worksheet.write_row(row_data, &types) {
                panic!("{e}");
            }

            while let Some(record) = get_next_record(&mut reader) {
                let row_data = record.iter().to_owned().collect();
                if let Err(e) = worksheet.write_row(row_data, &types) {
                    panic!("{e}");
                }
            }
        }

        if let Err(e) = worksheet.close() {
            panic!("{e}");
        }

        let final_buffer = workbook.finish().ok().unwrap();

        PyBytes::new_bound(py, &final_buffer.into_inner())
    }

    #[pyfn(m)]
    #[pyo3(name = "py_2d_to_xlsx")]
    fn py_2d_to_xlsx<'py>(
        py: Python<'py>,
        list: PyReadonlyArray2<'py, PyObject>,
    ) -> Bound<'py, PyBytes> {
        let ndarray = list.as_array();

        let ndarray_str = ndarray.mapv(|x| {
            if let Ok(inner_str) = x.extract::<String>(py) {
                inner_str
            } else {
                if let Ok(inner_num) = x.extract::<f64>(py) {
                    if inner_num.is_nan() {
                        String::from("")
                    } else {
                        inner_num.to_string()
                    }
                } else {
                    if let Ok(inner_date) = x.extract::<NaiveDateTime>(py) {
                        format!("{}", inner_date.format("%Y-%m-%d %r"))
                    } else {
                        String::from("")
                    }
                }
            }
        });

        let output_buffer = vec![];
        let mut workbook = WorkBook::new(Cursor::new(output_buffer));
        let mut worksheet = workbook.get_worksheet(String::from("Sheet 1"));

        for row in ndarray_str.rows() {
            let bytes = row.map(|x| x.as_bytes()).to_vec();
            if let Err(e) = worksheet.write_row(bytes) {
                panic!("{e}");
            }
        }

        if let Err(e) = worksheet.close() {
            panic!("{e}");
        }

        let final_buffer = workbook.finish().ok().unwrap();

        PyBytes::new_bound(py, &final_buffer.into_inner())
    }

    #[pyfn(m)]
    #[pyo3(name = "typed_py_2d_to_xlsx")]
    fn typed_py_2d_to_xlsx<'py>(
        py: Python<'py>,
        list: PyReadonlyArray2<'py, PyObject>,
        types: Bound<'py, PyList>,
    ) -> Bound<'py, PyBytes> {
        let ndarray = list.as_array();

        let ndarray_str = ndarray.mapv(|x| {
            if let Ok(inner_str) = x.extract::<String>(py) {
                inner_str
            } else {
                if let Ok(inner_num) = x.extract::<f64>(py) {
                    if inner_num.is_nan() {
                        String::from("")
                    } else {
                        inner_num.to_string()
                    }
                } else {
                    if let Ok(inner_date) = x.extract::<NaiveDateTime>(py) {
                        format!("{}", chrono_to_xlsx_date(inner_date))
                    } else {
                        String::from("")
                    }
                }
            }
        });

        let xlsx_types: Vec<&'static str> = types.iter().map(|x| {
            match x.extract::<String>().unwrap().as_str() {
                "n" => TYPE_NUMBER,
                "d" => TYPE_DATE,
                _ => TYPE_STRING
            }
        }).collect();

        let output_buffer = vec![];
        let mut workbook = WorkBook::new(Cursor::new(output_buffer));
        let mut worksheet = workbook.get_typed_worksheet(String::from("Sheet 1"));

        for row in ndarray_str.rows() {
            let bytes = row.map(|x| x.as_bytes()).to_vec();
            if let Err(e) = worksheet.write_row(bytes, &xlsx_types) {
                panic!("{e}");
            }
        }

        if let Err(e) = worksheet.close() {
            panic!("{e}");
        }

        let final_buffer = workbook.finish().ok().unwrap();

        PyBytes::new_bound(py, &final_buffer.into_inner())
    }

    m.add_class::<PyPostgresClient>()?;

    Ok(())
}
