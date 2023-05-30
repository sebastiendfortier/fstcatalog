from setuptools import setup, find_packages

with open('VERSION', encoding='utf-8') as f:
    version = f.read()
release = version


setup(
    name='fstcatalog',
    version=release,
    description='Library of functions for working with FST files',
    author='Sebastien Fortier',
    author_email='sebastien.fortier@ec.gc.ca',
    packages=find_packages(),
    install_requires=[
        'cartopy>=0.21.0',
        'cmcdict @ git+https://gitlab.science.gc.ca/CMDS/cmcdict.git',
        'fstd2nc @ git+https://github.com/neishm/fstd2nc.git',
        'fstpy @ git+https://gitlab.science.gc.ca/CMDS/fstpy.git',
        'geoviews>=1.9.6',
        'hvplot>=0.8.2',
        'intake>=0.6.8',
        'numpy>=1.23.5',
        'pandas>=1.5.3',
        'pyproj>=3.3.1',
        'xarray>=2023.2.0'
    ],
    package_data = {
    'fstcatalog': ['VERSION'],
  }
)
