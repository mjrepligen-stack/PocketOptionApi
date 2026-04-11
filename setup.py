"""The python wrapper for PocketOption API package setup."""
from setuptools import (setup, find_packages)

setup(
    name="pocketoptionapi",
    version="0.0.1",
    packages=find_packages(),
    # Runtime imports (stable_api, ws/client, time_sync) — keep in sync with vendor/requirements.txt
    install_requires=[
        "requests",
        "websocket-client==0.56",
        "websockets>=12.0",
        "pandas>=2.1.4",
        "tzlocal>=5.2",
        "backoff>=2.2.1",
    ],
    extras_require={"dev": ["pylint"]},
    include_package_data=True,
    description="Best Pocket Option API for python",
    long_description="Best Pocket Option API for python",
    url="",
    author="chema-creator",
    author_email="",
    zip_safe=False
)
