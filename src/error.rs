use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransmutaError {
    #[error("IO错误: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Excel解析错误: {0}")]
    ExcelError(String),

    #[error("CSV解析错误: {0}")]
    CsvError(#[from] csv::Error),

    #[error("Serde JSON错误: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("Arrow错误: {0}")]
    ArrowError(String),

    #[error("Parquet错误: {0}")]
    ParquetError(String),

    #[error("不支持的输出格式: {0}")]
    UnsupportedFormat(String),

    #[error("文件格式错误: {0}")]
    FileFormatError(String),

    #[error("数据处理错误: {0}")]
    DataProcessingError(String),

    #[error("参数错误: {0}")]
    InvalidArgument(String),
}

// 实现从calamine错误到我们的错误类型的转换
impl From<calamine::Error> for TransmutaError {
    fn from(err: calamine::Error) -> Self {
        TransmutaError::ExcelError(err.to_string())
    }
}

// 实现从arrow错误到我们的错误类型的转换
impl From<arrow::error::ArrowError> for TransmutaError {
    fn from(err: arrow::error::ArrowError) -> Self {
        TransmutaError::ArrowError(err.to_string())
    }
}

// 实现从parquet错误到我们的错误类型的转换
impl From<parquet::errors::ParquetError> for TransmutaError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        TransmutaError::ParquetError(err.to_string())
    }
}

// 实现从XlsxError到我们的错误类型的转换
impl From<calamine::XlsxError> for TransmutaError {
    fn from(err: calamine::XlsxError) -> Self {
        TransmutaError::ExcelError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TransmutaError>; 