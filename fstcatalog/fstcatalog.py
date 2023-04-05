# -*- coding: utf-8 -*-
import os
import warnings
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from pathlib import Path

import fstd2nc
import fstpy
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
from pyproj.crs import CRS as pcc
from xarray import Dataset
from typing import Tuple
from cartopy.crs import PlateCarree
import hvplot.xarray

rmn.fstopt('MSGLVL','ERRORS')

warnings.simplefilter('ignore')

fstd2nc.stdout.streams = ('errors',)

_META_DATA = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]
"""Nomvars for the metadata fields in a fst file"""

_COLUMNS_TO_REMOVE = [ 'ip1_kind', 'ip2_dec', 'ip2_kind', 'ip2_pkind', 'ip3_dec', 'ip3_kind', 'ip3_pkind', 'surface', 'follow_topography', 'ascending', 'shape', 'datev', 'lng', 'swa', 'key', 'deet', 'npas', 'datyp', 'nbits', 'd']
"""List of columns that get dropped to clean the dataframe"""

_DICT_PATH = '/home/smco502/datafiles/constants/opdict/ops.variable_dictionary.xml'
"""Operational dictionnary xml file path"""

class FstCatalogError(Exception):
    pass

class FstCatalog:
    """This class takes a list of files and catalogs (filters and sorts) all fst records"""

    def __init__(self, files: "Path|str|list[Path]|list[str]") -> None:
        """init instance"""
        self._files = files
        if isinstance(self._files, Path):
            self._files = [str(self._files.absolute())]
        elif isinstance(self._files, str):
            self._files = [os.path.abspath(str(self._files))]
        elif isinstance(self._files, list):
            self._files = [os.path.abspath(str(f)) for f in files]

        else:
            raise FstCatalogError('Filenames must be str or Path|str|list[Path]|list[str]\n')
        self._filter_fst_files()

    @property
    def files(self) -> list:
        """Gets the private attribute `_files`.

        :return: the list of file paths that where used to create the catalog.
        :rtype: list
        """
        return self._files

    @property
    def df(self) -> pd.DataFrame:
        """Gets the private attribute `_df`.

        :return: the dataframe containing record information.
        :rtype: pd.DataFrame
        """
        return self._df

    def _filter_fst_files(self, num_proc: int = 8) -> None:
        """filter out files that are not of fst type in parallel

        :param files: a list of files to filter
        :type files: list
        :param num_proc: number of processes to use, defaults to 8
        :type num_proc: int, optional
        """
        with Pool(min(num_proc,len(self._files))) as pool:
            filtered_files = pool.map(fstpy.std_io.maybeFST, self._files)
            files = np.where(filtered_files, self._files,'').tolist()
            self._files = [f for f in files if f != '']

    def catalog(self, num_proc : int = 8) -> None:
        """catalogs all records for fst files in parallel

        :param files: a list of fst file files to search in
        :type files: list
        :param num_proc: number of processes to use, defaults to 8
        :type num_proc: int, optional
        """
        grouping = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'grtyp', 'grid', 'level_unit']


        with Pool(min(num_proc,len(self._files))) as p:
            results = p.map(FstCatalog._get_fst_file_records_index, self._files)
            df = pd.concat(results, ignore_index=True)

            results = p.map(FstCatalog._aggregate_dataframe_columns, [group.reset_index(drop=True) for _, group in df.groupby(grouping)])
            df = pd.concat(results, ignore_index=True)

            derived_values = p.map(FstCatalog._get_record_crs_info, [row for _, row in df.iterrows()])

        df[['crs_cf_name', 'WKT']] = pd.DataFrame(derived_values)
        root = FstCatalog.__parse_opt_dict()
        descriptions_df = pd.DataFrame([FstCatalog._get_description_and_units_from_optdict(nomvar, root) for nomvar in df.nomvar.unique().tolist()])
        self._df = df.merge(descriptions_df[['nomvar', 'description', 'unit']], on='nomvar', how='left')

        return self


    def voir_view(self) -> pd.DataFrame:
        """Returns the a subset of columns of the dataframe records that correspond to a 'voir'

        :return: subset of columns of the dataframe records that correspond to a 'voir'
        :rtype: pd.DataFrame
        """
        return self._df[['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
        'ip3', 'grtyp', 'ig1', 'ig2', 'ig3',
        'ig4']]

    def advanced_view(self) -> pd.DataFrame:
        """Returns the a subset of columns of the dataframe records that have decoded values

        :return: subset of columns of the dataframe records that have decoded values
        :rtype: pd.DataFrame
        """
        return self._df[['nomvar', 'description', 'unit', 'date_of_observation', 'date_of_validity', 'level', 'level_unit', 'interval']]

    def get_dataset(self, index: int) -> Dataset:
        """gets a selected record from the catalog as a xarray dataset

        :param index: catalog index for a row
        :type index: int
        :raises FstCatalogError: Index out of range
        :return: xarray dataset of the selected record
        :rtype: xarray.Dataset
        """
        if index in self._df.index:
            ds = fstd2nc.Buffer(self._df.loc[index].path.split(','), vars=[f'{self._df.loc[index].nomvar}'], filter = FstCatalog._get_fstd2nc_search_filter(self._df.loc[index]), rpnstd_metadata=True, opdict=True ).to_xarray()
            ds = ds.assign(WKT=self._df.loc[index].WKT)
            ds = ds.assign(proj4=pcc.from_wkt(self._df.loc[index].WKT).to_proj4())
            return ds
        else:
            raise FstCatalogError("Invalid record index")

    def get_dataset_with_crs_info(self, index: int) -> Tuple[Dataset, str, str]:
        """gets a selected record accompanied with from the catalog as a xarray dataset

        :param index: catalog index for a row
        :type index: int
        :raises FstCatalogError: Index out of range
        :return: xarray dataset of the selected record, crs_cf_name and proj4 string  
        :rtype: Tuple[Dataset, str, str]
        """
        ds = self.get_dataset(index)
        crs_cf_name=self._df.loc[index].crs_cf_name
        proj4=pcc.from_wkt(self._df.loc[index].WKT).to_proj4()
        return ds, crs_cf_name, proj4


    @staticmethod
    def _get_fst_file_records_index(filename: str) -> None:
        """Helper function to call fstpy.StandardFileReader

        :param filename: a file name to read
        :type filename: str
        """
        filename = os.path.abspath(filename)
        df = fstpy.StandardFileReader(filename).to_pandas()
        df['path'] = filename
        df = fstpy.add_columns(df, columns = ['dateo', 'datev', 'ip_info'])
        df.drop(columns=_COLUMNS_TO_REMOVE, inplace=True, errors='ignore')
        df = df.loc[~df.nomvar.isin(_META_DATA)]
        df = df.rename(columns={'ip1_pkind': 'level_unit'})
        return df


    @staticmethod
    def _aggregate_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregates specific columns in to comma separated strings

        :param df: Catalog
        :type df: pd.DataFrame
        :return: DataFrame with aggregated columns
        :rtype: pd.DataFrame
        """
        newdf = pd.DataFrame([df.iloc[0]])
        newdf.reset_index(drop=True, inplace=True)
        newdf.at[0,'level'] = ','.join(np.sort(df.level.unique()).astype(str).tolist())
        newdf.at[0,'ip1'] = ','.join(np.sort(df.ip1.unique()).astype(str).tolist())
        newdf.at[0,'ip2'] = ','.join(np.sort(df.ip2.unique()).astype(str).tolist())
        newdf.at[0,'ip3'] = ','.join(np.sort(df.ip3.unique()).astype(str).tolist())
        newdf.at[0,'date_of_observation'] = ','.join(np.sort(df.date_of_observation.unique()).astype(str).tolist())
        newdf.at[0,'date_of_validity'] = ','.join(np.sort(df.date_of_validity.unique()).astype(str).tolist())
        newdf.at[0,'path'] = ','.join(np.sort(df.path.unique()).tolist())
        newdf.at[0,'interval'] = ','.join(np.sort(df.interval.unique()).astype(str).tolist())
        return newdf




    @staticmethod
    def _get_record_crs_info(row) -> pd.DataFrame:
        """for a catalog row, gets the WKT and cf crs name

        :param df: Catalog
        :type df: pd.DataFrame
        :return: Catalog with the crs_cf_name and WKT columns added
        :rtype: pd.DataFrame
        """
        nomvar = row.nomvar
        first_file = row.path.split(',')[0]

        ds = fstd2nc.Buffer(
            first_file, 
            vars=[nomvar],
            filter=FstCatalog._get_fstd2nc_search_filter(row)
            ).to_xarray() 

        crs_cf_name = ''
        wkt = ''
        if nomvar[0].isdigit():
            nomvar = '_'.join(['',nomvar])
        if len(ds):
            grid_mapping = ds[nomvar].attrs.get('grid_mapping')
            crs_cf_name = grid_mapping
            wkt = pcc.from_cf(ds[grid_mapping].attrs).to_wkt()

        result = pd.Series([crs_cf_name, wkt])
        
        return result
        
    @staticmethod
    def __parse_opt_dict() -> ET.ElementTree:
        """Gets the root element of the operation fst var dictionnary

        :return: tree root
        :rtype: ET.ElementTree
        """
        tree = ET.parse(f'{_DICT_PATH}')

        # Get the root element
        return tree.getroot()


    @staticmethod
    def _get_description_and_units_from_optdict(nomvar: str, root: ET.ElementTree) -> dict:
        """Searches in the operational dictionnary for the corresponding description and units of a specific nomvar

        :param nomvar: nomvar to search for in the op dict
        :type nomvar: str
        :param root: tree root of the opt dict
        :type root: ET.ElementTree
        :return: dictionnary of the nomvar, description and unit of the found values
        :rtype: dict
        """

        nomvar_info = {
                'nomvar': '',
                'description': '',
                'unit': ''
            }

        # Find the metvar element with the given nomvar
        nomvar_metvar = root.find(f".//metvar[nomvar='{nomvar}']") #.format(nomvar)

        # check if the nomvar element has a usage attribute with value 'current'
        if nomvar_metvar.get('usage') == 'current':

            # Get the short English description
            nomvar_short_en = nomvar_metvar.find(".//short[@lang='en']")

            # Get the units
            nomvar_units = nomvar_metvar.find(".//units")

            # Create a dictionary with the information
            nomvar_info = {
                'nomvar': nomvar,
                'description': nomvar_short_en.text if not (nomvar_short_en is None) else '',
                'unit': nomvar_units.text if not (nomvar_units is None) else ''
            }

        return nomvar_info
    
    def get_hvplot(self, index: int):
        if index in self._df.index:
            crs_in = PlateCarree()
            project_in = False
            ds, grid_name, proj4_in = self.get_dataset_with_crs_info(index)
            if grid_name == 'rotated_pole':
                crs_in = proj4_in
                project_in = True
            
            return ds.hvplot.quadmesh(
                x='lon',
                y='lat',
                rasterize=True,
                data_aspect=1,
                frame_height=550,
                cmap='jet',
                crs=crs_in,
                projection=PlateCarree(),
                project=project_in,
                geo=True,
                coastline=True,
                widget_location='bottom'
            )
        else:
            raise FstCatalogError(f"Invalid record index: {index}")
        
        
    @staticmethod
    def _get_fstd2nc_search_filter(row) -> list:
        filter = [
                f"typvar=='{row.typvar}'",
                f"etiket=={row.etiket.ljust(12).encode('utf8')!r}",
                f"np.isin(ip1, {list(map(int,row.ip1.split(',')))})",
                f"np.isin(ip2, {list(map(int,row.ip2.split(',')))})",
                f"np.isin(ip3, {list(map(int,row.ip3.split(',')))})",
                f'ni=={row.ni}',
                f'nj=={row.nj}',
                f'nk=={row.nk}',
                f"grtyp=='{row.grtyp}'"
                ]
        # display(filter)
        return filter
