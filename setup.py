import os
from setuptools import find_packages, setup

setup(
    name='etherdata',
    version='1.0.0',
    long_description_content_type='text/markdown',
    packages=find_packages(exclude=['schemas', 'tests']),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    keywords='ethereum',
    python_requires='>=3.7.2,<4',
    install_requires=[
        'web3>=5.29,<6',
        'eth-utils==1.10',
        'eth-abi==2.1.1',
        'python-dateutil>=2.8.0,<3',
        'click==8.0.4',
        'ethereum-dasm==0.1.4',
        'base58',
        'requests'
    ],
    extras_require={
        'streaming': [
            'timeout-decorator==0.4.1',
            'google-cloud-pubsub==2.1.0',
            'google-cloud-storage==1.33.0',
            'pulsar-client==2.10.0',
            'kafka-python==2.0.2',
            'sqlalchemy==1.4',
            'pg8000==1.16.6',
            'libcst==0.3.21'
            'boto3==1.18.11',
        ],
        'dev': [
            'pytest~=4.3.0'
        ]
    }
)
