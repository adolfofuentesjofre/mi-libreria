from setuptools import find_packages, setup

# To use a consistent encoding
from codecs import open
from os import path

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='mypythonlib', # nombre de la libreria
    packages=find_packages(include=['mypythonlib']), # carpeta donde buscar la libreria
    version='0.4.3',
    description='Mi libreria',
    long_description_content_type='text/markdown',
    author='Adolfo',
    install_requires=[
        'pandas>=1.3.2',
        'numpy>=1.21',
        'holidays>=0.10.1',
        'scipy>=1.4.1',
        'pyodbc>=4.0.32',
        'multiprocess>=0.70.12.2'
        ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==6.2.4'],
    test_suite='tests',
    classifiers=[
        "Intended Audience :: Data Scientists and Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8"],
    include_package_data=True,
    package_data={'': ['data/*.sav', 'data/*.joblib', 'data/*.csv']},
)