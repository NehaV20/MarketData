from setuptools import setup, find_packages

setup(
    name="marketdataSolution",
    version="0.1.0",
    description="SDK for Market Data Solution API",
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        'requests',
        'websocket-client',
        'protobuf',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6'
)
