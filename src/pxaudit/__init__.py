try:
    from importlib.metadata import version as _metadata_version

    __version__ = _metadata_version("pxaudit")
except Exception:
    __version__ = "0.0.0"

_PRIDE_PREFIX = "PXD"
