from setuptools import find_packages, setup

setup(
    name='mypythonlib', # nombre de la libreria
    packages=find_packages(include=['mypythonlib']), # carpeta donde buscar la libreria
    version='0.3.0',
    description='Mi libreria',
    author='Adolfo',
    install_requires=[
        'pandas>=1.3.2',
        'numpy>=1.21',
        'holidays>=0.10.1',
        'scipy>=1.4.1',
        'pyodbc>=4.0.32',
        ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==6.2.4'],
    test_suite='tests',
    classifiers=[
        "Intended Audience :: Data Scientists and Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8"]
)