from setuptools import setup, find_packages

setup(
    name="vman-admin",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "typer[all]",
    ],
    entry_points={
        "console_scripts": [
            "vman=app.cli:main",
        ],
    },
)