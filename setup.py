"""
Flask-SAPB1
-------------
## Synopsis
The python flask extension is used to connect the SAP B1 DI COM object to perform all of the data related functions.

## Configuration

#### DIAPI
SAP B1 DI COM object.  You have to specify which version is installed in the SAP B1 server and will be loaded for the program.

  * SAPbobsCOM90
  * SAPbobsCOM89
  * SAPbobsCOM88
  * SAPbobsCOM67
  * SAPbobsCOM2007
  * SAPbobsCOM2005

#### SERVER
SAP B1 Server name or IP address.

#### LANGUAGE
Specify the default language for the company.

#### DBSERVERTYPE
Specify MS SQL server version.

#### COMPANYDB
The company database name,

#### B1USERNAME
The SAP B1 user username for the connection.

#### B1PASSWORD
The SAP B1 user password for the connection.

#### DBUSERNAME
The username for the company database.

#### DBPASSWORD
The password for the company password.
"""
from setuptools import find_packages, setup

setup(
    name='Flask-SAPB1',
    version='0.0.2',
    url='https://github.com/ideabosque/Flask-SAPB1',
    license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='Use to connect SAP B1 DI API.',
    long_description=__doc__,
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='win32',
    install_requires=['Flask', 'pymssql', 'pywin32'],
    download_url = 'https://github.com/ideabosque/Flask-SAPB1/tarball/0.0.2',
    keywords = ['SAP B1', 'SAP Business One', 'DI'], # arbitrary keywords
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Framework :: Flask',
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
