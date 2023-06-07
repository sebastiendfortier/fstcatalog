#!/bin/bash
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR=${DIR:0:${#DIR}-3}
echo $ROOT_DIR
cd ${DIR}
VERSION=$(cat ${ROOT_DIR}fstcatalog/__init__.py | grep __version__ | awk -F\' '{print $2}')
PLAT=all

name=fstcatalog
PKGNAME=${name}_${VERSION}_${PLAT}
echo ${VERSION}
echo ${DIR}
echo ${name}
echo ${PKGNAME}

rm -rf ${ROOT_DIR}/${name}/__pycache__


echo '{' > control.json
echo '    "name": "'${name}'",' >> control.json
echo '    "version": "'${VERSION}'",' >> control.json
echo '    "platform": "'${PLAT}'",' >> control.json
echo '    "maintainer": "CMDS",' >> control.json
echo '    "description": "'${name}' package",' >> control.json
echo '    "x-build-date": "'`date`'",' >> control.json
echo '    "x-build-platform": "'${BASE_ARCH}'",' >> control.json
echo '    "x-build-host": "'`hostname -f`'",' >> control.json
echo '    "x-build-user": "'${USER}'",' >> control.json
echo '    "x-build-uname": "('`uname -s`', '`uname -n`', '`uname -r`', '`uname -v`', '`uname -m`')"' >> control.json
echo '}' >> control.json



echo 'Building package '${PKGNAME}
mkdir -p ${PKGNAME}/lib/python/${name}
mkdir -p ${PKGNAME}/.ssm.d
mkdir -p ${PKGNAME}/bin
mkdir -p ${PKGNAME}/share
mkdir -p ${PKGNAME}/etc/profile.d


PROJECT_ROOT=$ROOT_DIR/${name}
echo 'Copying files to '${PKGNAME}' directory'
# cp ssm_package_setup.sh ${PKGNAME}/etc/profile.d/${PKGNAME}.sh
cp control.json ${PKGNAME}/.ssm.d/.
cp -rf ${PROJECT_ROOT}/* ${PKGNAME}/lib/python/${name}/.
# cp -rf requirements.txt ${PKGNAME}/share/.
echo 'Creating ssm archive '${PKGNAME}'.ssm'
tar -zcvf ${PKGNAME}.ssm ${PKGNAME}
echo 'Cleaning up '${PKGNAME}' directory'
rm -rf control.json
rm -rf ${PKGNAME}/

chmod 755 ${PKGNAME}.ssm
cp ${PKGNAME}.ssm /home/${USER}/public/ssm/.
mv ${PKGNAME}.ssm /tmp/${USER}/.


echo ""
read -p "Do you want to publish (if you are Sebas)? [Y|N]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    SSM_BASE=/fs/site5/eccc/cmd/w/sbf000/ssm
    echo 'ssm domain is '${SSM_BASE}
    echo 'unpublish old package'
    ssh sbf000@ppp5 source .profile&&ssm unpublish -d ${SSM_BASE}/${name}/${VERSION} -p ${PKGNAME} -pp ${PLAT}
    echo 'uninstall old package'
    ssh sbf000@ppp5 source .profile&&ssm uninstall -d ${SSM_BASE}/master -p ${PKGNAME}

    #ssm created -d ${SSM_BASE}/master
    echo 'Installing package to '${SSM_BASE}'/master'
    ssh sbf000@ppp5 source .profile&&ssm install -d ${SSM_BASE}/master -f /tmp/${USER}/${PKGNAME}.ssm
    echo 'Create domain '${SSM_BASE}'/'${name}'/'${VERSION}
    ssh sbf000@ppp5 source .profile&&ssm created -d ${SSM_BASE}/${name}/${VERSION}
    echo 'Publishing package '${PKGNAME}' to '${SSM_BASE}'/'${name}'/'${VERSION}
    ssh sbf000@ppp5 source .profile&&ssm publish -d ${SSM_BASE}/master -P ${SSM_BASE}/${name}/${VERSION} -p ${PKGNAME} -pp ${PLAT}

    rm /tmp/${USER}/${PKGNAME}.ssm

    echo 'Execute . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/'${name}'/'${VERSION}'/ to use official package'
    # echo 'Execute . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/'${name}'/'${VERSION}'/ to use official package'
    echo 'Execute . ssmuse-sh -d '${SSM_BASE}'/'${name}'/'${VERSION}' to use this package'
else
    echo ""
    echo "Package is here: /tmp/${USER}/${PKGNAME}.ssm"
    echo ""
fi
