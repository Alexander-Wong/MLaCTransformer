"""Transformers module."""


class Transformers:
    """Applies transformations to data."""

    def __init__(self, yaml_path: str) -> None:
        self.yaml_path = yaml_path

    def run(self) -> None:
        print("Hi from Transformers")
