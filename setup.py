from setuptools import setup, find_packages

with open("README.md", "r") as f:
    readme = f.read()

setup(
    name='megaton_data',
    version='1.1.9',
    author='Makoto Shimizu',
    author_email='aa.analyst.ga@gmail.com',
    description='Python utilities for GCP and Pardot APIs.',
    long_description=readme,
    long_description_content_type='ext/markdown',
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Internet",
    ],
    python_requires='>=3.7',
)
