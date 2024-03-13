# Introduction

    Helper functions for Prediktor projects

The included classes and functions allows for easier and more maintainable code when
working with Prediktor projects. The main features are:

- Communication with PowerView Data Warehouse
- Sending emails
- Working with FTP and SFTP server
- Logging
- Generating files through templates

Install is primarily done through PyPi with `pip install pyPrediktorUtilities`.

Read the documentation here: [https://prediktoras.github.io/pyPrediktorUtilities/](https://prediktoras.github.io/pyPrediktorUtilities/)

# Setup

## Setup for development (use PyPi for installation)

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

## Possible issues

### PyODBC

When running `tox` command you may experience issues with ODBC driver. A possible solution is to rebuild pyodbc:

```
pip uninstall pyodbc
pip install --force-reinstall --no-binary :all: pyodbc
```

on Mac:

```
export LDFLAGS="-L/opt/homebrew/lib"
export CPPFLAGS="-I/opt/homebrew/include"
.tox/default/bin/pip install --force-reinstall --no-binary :all: pyodbc
```

The last commands adjust the environment variables to point to your Homebrew
installation paths and then rebuild pyodbc. In that way we make sure
the build environment knows where to find the unixODBC headers and libraries.
Also, make sure to run these commands within the activated tox environment.

# DWH

## Introduction

Helper functions to access a PowerView Data Warehouse or other SQL databases.
This class is a wrapper around pyodbc and you can use all pyodbc methods as well as those provided by pyodbc. Look at the pyodbc documentation and use the cursor attribute to access the pyodbc cursor.
You can use it as a singleton or as a regular object. If you need to access only one database in the entire application,
you should use the singleton version.

## Requirements

You have to install ODBC drivers on your machine!

## Initialisation

When initialising Dwh you have to pass the patameters below following the same order:

- SQL_SERVER
- SQL_DATABASE
- SQL_USER
- SQL_PASSWORD

```
dwh = Dwh(SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD)
# or
dwh = DwhSingleton(SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD)
```

There is fifth parameter `SQL_DRIVER_INDEX` but it is not required. If you do not pass the driver index, pyPrediktorMapClient is going to check two things:

- All drivers supported by `pyodbc`.
- All drivers that are supported by `pyodbc` and are also installed and available on your machine.

pyPrediktorMapClient is going to choose the first one from the list of supported and available drivers. In that way, the whole process is automated for you and you can use pyPrediktorMapClient Dwh class out of the box.

If you would like to pick up a particular driver you have to do the above yourself and pass the desired driver index. Here is an example how to do that. First we get a list of available drivers supported by `pyodbc`:

```
available_drivers = pyodbc.drivers()
```

Let's say that list contains the following drivers:

```
[
    'ODBC Driver 18 for SQL Server',
    'ODBC Driver 13 for SQL Server',
    'ODBC Driver 17 for SQL Server'
]
```

and you'd like to use the third one. Threfore the driver index you have to pass when initialising Dwh equals 2 _(because that is the index of the driver in the list)_:

```
driver_index = 2
dwh = Dwh(SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD, driver_index)
```

## Example usage

```
from pyprediktorutilities.dwh.dwh import Dwh

dwh = Dwh("localhost", "mydatabase", "myusername", "mypassword")
results = dwh.fetch("SELECT * FROM mytable")
dwh.execute("INSERT INTO mytable VALUES (1, 'test')")
```

# TODOs

1. In `setup.cfg` file there is the following code snipped:

```
install_requires =
    pytest
    pytest-cov
    pytest-mock
    pyarrow
```

Install these packages only when testing and build the documentation via `tox -e docs`. Do not install them on production.
