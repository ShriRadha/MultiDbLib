from setuptools import setup, find_packages

setup(
    name='MultiDBLib',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'pymongo',
        'psycopg2-binary',
        'mysql-connector-python'
    ],
    description='A simple database interaction library for MongoDB, PostgreSQL, and MySQL',
)
