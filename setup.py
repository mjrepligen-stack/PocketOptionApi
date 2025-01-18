"""The python wrapper for PocketOption API package setup."""
from setuptools import (setup, find_packages)

setup(
    name="pocketoptionapi",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["pylint", "requests", "websocket-client==0.56"],
    include_package_data=True,
    description="Best IQ Option API for python",
    long_description="Best IQ Option API for python",
    url="",
    author="chema2411",
    author_email="",
    zip_safe=False
)
