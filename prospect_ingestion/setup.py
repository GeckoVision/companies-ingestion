from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='prospect_ingestion',
    version='1.0.0',
    author='Ernani de Britto Murtinho',
    author_email='ernani.britto@geckovision.pro',
    description='PROSPECT_INGESTION: Job to automatize B2B data ingestion',
    long_description=long_description,
    python_requires='>=3.7',
    platforms='MacOS X; Linux',
    packages=find_packages(),
    install_requires=(
        'pex',
    ),
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
    ],
)
