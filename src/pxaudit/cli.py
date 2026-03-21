"""Command-line interface for pxaudit."""

import click


@click.group()
def main() -> None:
    """Audit Proteomics Exchange study metadata."""
