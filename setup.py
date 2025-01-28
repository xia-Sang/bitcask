from setuptools import setup, find_packages

setup(
    name="bitcask",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "BTrees>=4.11.3",
    ],
    author="sang xia",
    description="A Python implementation of the Bitcask storage engine",
    keywords="database, storage, key-value store",
    python_requires=">=3.8",
)
