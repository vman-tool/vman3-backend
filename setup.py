from setuptools import setup, find_packages

setup(
    name="vman-admin",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "typer>=0.12.3",
        "click>=8.0.0",
        "rich>=10.11.0",
        "shellingham>=1.3.0",
    ],
    entry_points={
        "console_scripts": [
            "vman=app.cli.cli:app",
        ],
    },
    python_requires=">=3.10",
    options={
        "bdist_wheel": {
            "universal": False
        }
    }
)