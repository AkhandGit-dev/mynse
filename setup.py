from setuptools import setup, find_packages

setup(
    name='mynse',
    version='0.1.0',
    description='Custom NSE F&O data library',
    author='Akhand Pratap Singh',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pandas',
        'tabulate',
        'colorama',
        'pytz'
    ],
    python_requires='>=3.8',
)
