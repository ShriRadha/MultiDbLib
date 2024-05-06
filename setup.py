from setuptools import setup, find_packages

setup(
    name='MultiDBLib',
    version='1.0',
    package_dir={'': 'app'},
    packages=find_packages(where='app'),
    install_requires=[
        'pymongo',
        'psycopg2-binary',
        'mysql-connector-python'
    ],
    description='A simple database interaction library for MongoDB, PostgreSQL, and MySQL',
)
 