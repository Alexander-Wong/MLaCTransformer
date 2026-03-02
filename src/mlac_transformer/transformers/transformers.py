"""Transformers module."""


class Transformers:
    """Applies transformations to data."""

    def __init__(self, raw_path: str, yaml_path: str) -> None:
        self.raw_path = raw_path
        self.yaml_path = yaml_path

    def run(self) -> None:
        print("Hi from Transformers")
