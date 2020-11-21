import setuptools
from pkg_resources import parse_requirements

with open("README.md", "r") as fh:
    long_description = fh.read()

install_reqs = parse_requirements("requirements.txt")

setuptools.setup(
    name="temporal-python-sdk",
    version="1.0.0-beta1",
    author="Mohammed Firdaus",
    author_email="firdaus.halim@gmail.com",
    description="Unofficial Python SDK for the Temporal Workflow Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/firdaus/temporal-python-sdk",
    packages=setuptools.find_packages(exclude=["cadence", "tests", "cadence.*", "tests.*"]),
    install_requires=[
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True
)
