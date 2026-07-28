class DataViewerError(Exception):
    status_code = 400

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class UnsupportedFileTypeError(DataViewerError):
    status_code = 415


class FileTooLargeError(DataViewerError):
    status_code = 413


class GeospatialDataError(DataViewerError):
    status_code = 422
