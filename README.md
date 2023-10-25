# pyPrediktorUtilities


    Helper functions for Prediktor projects


The included classes and functions allows for easier and more maintainable code when
working with Prediktor projects. The main features are:

* Sending emails
* Working with FTP and SFTP server
* Logging
* Generating files through templates

Install is primarily done through PyPi with `pip install pyPrediktorUtilities`. If you want to contribute or need
run the Jupyter Notebooks in the `notebooks` folder locally, please clone this repository.

Read the documentation here: [https://prediktoras.github.io/pyPrediktorUtilities/](https://prediktoras.github.io/pyPrediktorUtilities/)

## Setup to Install
1. First clone the repository and navigate to the main folder of repository.
```
git clone git@github.com:PrediktorAS/pyPrediktorUtilities.git
```
2. Create Virtual environment
```
python3 -m venv .venv
source .venv/bin/activate
```
3. Install dependencies
As this is a python package, dependencies are in setyp.py (actually in setup.cfg, as this is a pyScaffold project). Requirements.txt will perform the correct installation and add a couple of
additional packages
```
pip install -r requirements.txt
```
4. Run tests
```
tox
```
5. Do your changes
Add whatever you need and create PRs to be approved
6. Build
```
tox -e build
```
7. Publish to PyPi test and live
```
tox -e publish
tox -e publish -- --repository pypi
```
