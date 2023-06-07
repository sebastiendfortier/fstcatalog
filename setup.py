#######################################################################
#
# Copyright (c) 2023 Government of Canada
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#######################################################################

from pathlib import Path
import re
from setuptools import find_packages, setup


def read(filename, encoding='utf-8'):
    """read file contents"""

    fullpath = Path(__file__).resolve().parent / filename

    with fullpath.open() as fh:
        contents = fh.read().strip()
    return contents


def get_package_version():
    """get version from top-level package init"""

    version_file = read('fstcatalog/__init__.py')
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)

    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')


LONG_DESCRIPTION = read('README.md')

DESCRIPTION = 'Python library to catalog FSTD file contents'

MANIFEST = Path('MANIFEST')

if MANIFEST.exists():
    MANIFEST.unlink()


setup(
    name='fstcatalog',
    version=get_package_version(),
    description=DESCRIPTION.strip(),
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    license='GPLv3',
    platforms='all',
    keywords=' '.join([
        'cmc',
        'intake',
        'fstd',
        'fstd2nc',
        'catalog'
    ]),
    author='Sebastien Fortier',
    author_email='sebastien.fortier@ec.gc.ca',
    packages=find_packages(),
    include_package_data=True,
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
    ]
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python'
    ]
)
