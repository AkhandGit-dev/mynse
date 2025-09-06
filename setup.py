from setuptools import setup, find_packages

setup(
    name="mynse",
    version="0.1.0",
    packages=find_packages(),  # finds the inner mynse folder
    install_requires=[
        "requests",
        "pandas",
        "tabulate",
        "colorama",
        "pytz"
    ],
    python_requires=">=3.7"
)

