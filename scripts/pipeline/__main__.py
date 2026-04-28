"""Allow ``python -m pipeline`` to launch the Typer CLI."""

from .app import app


if __name__ == "__main__":
    app()
