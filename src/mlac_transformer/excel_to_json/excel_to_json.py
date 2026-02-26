"""ExcelToJson module."""


class ExcelToJson:
    """Transforms an Excel file to a JSON representation."""

    def __init__(self, excel_path: str) -> None:
        self.excel_path = excel_path

    def run(self) -> None:
        print("Hi from ExcelToJson")
