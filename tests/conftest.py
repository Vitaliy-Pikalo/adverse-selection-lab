import importlib.util

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa


def pytest_addoption(parser):
    group = parser.getgroup("spark parity")
    group.addoption(
        "--spark-full-data", action="store_true", default=False,
        help="Run parity on the audited full hourly dataset; requires PySpark and local data.",
    )
    group.addoption(
        "--spark-backfill", default="data/backfill",
        help="Full hourly backfill directory (relative paths resolve from the repository root).",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "spark_full: full hourly-data parity (explicit opt-in)")
    if config.getoption("--spark-full-data") and importlib.util.find_spec("pyspark") is None:
        raise pytest.UsageError(
            "--spark-full-data requires PySpark; run with the /opt/asl-spark/venv WSL Python."
        )


@pytest.fixture
def rsa_key_path(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    p = tmp_path / "test.pem"
    p.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return p


@pytest.fixture
def ed25519_key_path(tmp_path):
    key = ed25519.Ed25519PrivateKey.generate()
    p = tmp_path / "ed.txt"  # Kalshi downloads keys as .txt, PKCS8 PEM, LF endings
    p.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return p
