# -*- coding: utf-8 -*-
"""
This module provides a class for cataloging fst files into an intake catalog.

It also defines the following global variables:
- _META_DATA: Nomvars for the metadata fields in a fst file
- _COLUMNS_TO_REMOVE: List of columns that get dropped to clean the dataframe
"""


import os
import warnings
from multiprocessing import Pool
from pathlib import Path

import cartopy.crs as ccrs
import cmcdict
import fstd2nc
import fstpy
import geoviews.feature as gf
import hvplot.xarray
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
from intake.catalog import Catalog
from intake.catalog.local import Catalog, LocalCatalogEntry

rmn.fstopt('MSGLVL', 'ERRORS')

warnings.simplefilter('ignore')

fstd2nc.stdout.streams = ('errors',)

_META_DATA = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]
"""Nomvars for the metadata fields in a fst file"""

_COLUMNS_TO_REMOVE = ['ip1_kind', 'ip2_dec', 'ip2_kind', 'ip2_pkind', 'ip3_dec', 'ip3_kind', 'ip3_pkind', 'surface',
                      'follow_topography', 'ascending', 'shape', 'datev', 'lng', 'swa', 'key', 'deet', 'npas', 'datyp', 'nbits', 'd']
"""List of columns that get dropped to clean the dataframe"""


class FstCatalogError(Exception):
    pass


class FstCatalog:
    """This class takes a list of files and catalogs (filters and sorts) all fst records into an intake catalog

    :param files: fst files to catalog
    :type files: Path|str|list[Path]|list[str]
    :param file_filter: _description_, defaults to None
    :type file_filter: str, optional
    :raises FstCatalogError: _description_
    """

    def __init__(self, files: "Path|str|list[Path]|list[str]", by_var=True) -> None:
        """
        Initializes a new instance of the FstCatalog class.

        :param files: fst files to catalog
        :type files: Path|str|list[Path]|list[str]
        :param by_var: by variable, defaults to True
        :type by_var: bool, optional
        :raises FstCatalogError: if filenames are not str or Path|str|list[Path]|list[str]
        """
        self._files = files
        self._by_var = by_var
        if isinstance(self._files, Path):
            self._files = [str(self._files.absolute())]
        elif isinstance(self._files, str):
            self._files = [os.path.abspath(str(self._files))]
        elif isinstance(self._files, list):
            self._files = [os.path.abspath(str(f)) for f in files]

        else:
            raise FstCatalogError(
                'Filenames must be str or Path|str|list[Path]|list[str]\n')
        self._filter_fst_files()
        self._catalog()

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
        with Pool(min(num_proc, len(self._files))) as pool:
            filtered_files = pool.map(fstpy.std_io.maybeFST, self._files)
            files = np.where(filtered_files, self._files, '').tolist()
            self._files = [f for f in files if f != '']

    def _catalog(self, num_proc: int = 8) -> None:
        """catalogs all records for fst files in parallel

        :param files: a list of fst file files to search in
        :type files: list
        :param num_proc: number of processes to use, defaults to 8
        :type num_proc: int, optional
        """
        if self._by_var:
            grouping = ['nomvar', 'typvar', 'etiket', 'ni',
                        'nj', 'nk', 'grtyp', 'grid', 'level_unit']
        else:
            grouping = ['ni', 'nj', 'nk', 'grtyp', 'grid', 'level_unit']

        with Pool(min(num_proc, len(self._files))) as p:
            results = p.map(
                FstCatalog._get_fst_file_records_index, self._files)
            self._df = pd.concat(results, ignore_index=True)

        self._get_descriptions()
        self._df = self._df.applymap(str)
        self._aggregate_values(grouping)

        self._set_description()
        self._set_source()
        self._rename_columns()
        self._df['driver'] = 'fstd2nc'
        self._df['forecast_axis'] = True
        self._get_filter()
        self._select_columns()

    def _aggregate_values(self, grouping: list):
        """aggregate the values of the dataframe into lists

        :param grouping: list of columns to group on
        :type grouping: list
        """
        agg_func = {col: lambda x: ','.join(
            x.unique()) for col in self._df.columns if col not in grouping}
        self._df = self._df.groupby(grouping, as_index=False).agg(agg_func)


    def _get_descriptions(self):
        """Gets the description and units for all the nomvars"""
        descriptions_df = pd.DataFrame([cmcdict.get_metvar_metadata(nomvar) for nomvar in self._df.nomvar.unique(
        ).tolist() if cmcdict.get_metvar_metadata(nomvar) is not None])
        self._df = self._df.merge(descriptions_df[[
                      'nomvar', 'description_short_en', 'units']], on='nomvar', how='left')

    def _set_description(self):
        """Sets the descriptions in self.df"""
        self._df['description'] = self._df.apply(
            lambda row: str(row['description_short_en']) + ' ' + str(row['units']), axis=1)

    def _set_source(self):
        """Creates the source column from aggreagation of columns"""
        self._df['source'] = self._df.apply(
            lambda row: row['nomvar'].replace(',', '_') + '_' + row['etiket'] + '_' + row['level_unit'],  axis=1)
        self._df['source'] = self._df.apply(lambda row: row['source'].replace(',', '_'),  axis=1)
        self._number_dups()

    def _rename_columns(self):
        """Rename the orginal columns"""
        self._df = self._df.rename(
            columns={'path': 'urlpath', 'nomvar': 'vars'})

    def _get_filter(self):
        """Gets and set the filter column for the intake plugin"""
        self._df['filter'] = self._df.apply(
            FstCatalog._get_fstd2nc_search_filter, axis=1)

    def _select_columns(self):
        """Gets a subset of columns"""
        self._df = self._df[['source', 'description', 'driver',
                             'urlpath', 'vars', 'filter', 'forecast_axis']]

    def _number_dups(self):
        """If we have similar sources, add numbers to them"""
        # Identify the duplicate rows in the 'source' column
        self._df['is_duplicate'] = self._df.duplicated(
            subset='source', keep=False)

        # Create the automatic numbering for duplicates using groupby and cumcount
        self._df['auto_number'] = self._df.groupby('source').cumcount()

        # Combine the 'source' column and the 'auto_number' column for duplicates
        self._df['source'] = self._df.apply(
            lambda row: f"{row['source']}_{row['auto_number']}" if row['is_duplicate'] else row['source'], axis=1)

        # Drop the 'is_duplicate' and 'auto_number' columns
        self._df = self._df.drop(columns=['is_duplicate', 'auto_number'])

    def to_yaml(self, path: str):
        """Dumps the catalog to intake formatted yml

        :param path: path of the yml that will be written
        :type path: str
        """
        data_dict = self._df.to_dict(orient='records')

        with open(path, 'w') as f:
            f.write("sources:\n")
            for entry in data_dict:
                f.write(f"  {entry['source']}:\n")
                f.write(f"    description: {entry['description']}\n")
                f.write(f"    driver: {entry['driver']}\n")
                f.write(f"    args:\n")
                f.write(f"      urlpath: {entry['urlpath'].split(',')}\n")
                f.write(f"      vars: {entry['vars']}\n")
                f.write(f"      filter: {entry['filter']}\n")
                f.write(f"      forecast_axis: {entry['forecast_axis']}\n\n\n")

    def to_intake(self) -> Catalog:
        """Dumps the catalog to intake format object"""
        data_sources = {}
        data_dict = self._df.to_dict(orient='records')
        for entry in data_dict:
            entry_args = {
                'urlpath': entry['urlpath'].split(','),
                'vars': list(entry['vars']) if ',' in entry['vars'] else entry['vars'],
                'filter': entry['filter'],
                'forecast_axis': entry['forecast_axis']
            }
            entry_obj = {
                'description':  entry['description'],
                'driver': entry['driver'],
                'args': entry_args
            }
            data_sources[entry['source']] = entry_obj

        entries = {
            source_name: LocalCatalogEntry(
                name=source_name,
                description=source_data['description'],
                driver=source_data['driver'],
                args=source_data['args'],
                metadata={}
            )
            for source_name, source_data in data_sources.items()
        }

        mycat = Catalog.from_dict(entries)
        mycat.name = 'dynamic'
        return mycat

    @staticmethod
    def _get_fst_file_records_index(filename: str) -> None:
        """Helper function to call fstpy.StandardFileReader

        :param filename: a file name to read
        :type filename: str
        """
        filename = os.path.abspath(filename)
        df = fstpy.StandardFileReader(filename).to_pandas()
        df['path'] = filename
        df = fstpy.add_columns(df, columns=['dateo', 'datev', 'ip_info'])
        df.drop(columns=_COLUMNS_TO_REMOVE, inplace=True, errors='ignore')
        df = df.loc[~df.nomvar.isin(_META_DATA)]
        df = df.rename(columns={'ip1_pkind': 'level_unit'})

        return df

    @staticmethod
    def _get_fstd2nc_search_filter(row) -> list:
        """Create the filter that will be used by the intake plugin to select a particular variable"""
        filter = [
            f"typvar=='{row.typvar}'",
            f"etiket=='{row.etiket}'",
            f"np.isin(ip1, {list(map(int,row.ip1.split(',')))})",
            f"np.isin(ip2, {list(map(int,row.ip2.split(',')))})",
            f"np.isin(ip3, {list(map(int,row.ip3.split(',')))})",
            f'ni=={row.ni}',
            f'nj=={row.nj}',
            f'nk=={row.nk}',
            f"grtyp=='{row.grtyp}'"
        ]
        return filter


def combine_catalogs(catalogs: list):
    """Function to help combining intake catalogs

    :param catalogs: a list of intake catalogs
    :type catalogs: list
    :return: A nes catalog with all entries merged
    :rtype: intake.catalog.Catalog
    """
    # Assuming you have catalogs named 'cat1', 'cat2', and 'cat3'
    combined_cat = Catalog(name='combined')

    # Combine the catalogs by adding their entries
    for catalog in catalogs:
        for entry_name, entry in catalog.items():
            combined_cat[entry_name] = entry

    return combined_cat
 

def hvplot_cat_entry(dataset):
    """Generates an hvplot for the given xarray Dataset

    :param dataset: an xarray.Dataset of a single variable
    :type dataset: xarray.Dataset
    :return: an hvplot object
    :rtype: hvplot
    """

    if dataset is None:
        print("Error: unable to create hvplot due to missing dataset")
        return None

    data_vars = dataset.data_vars
    data_var = [var for var in data_vars if 2 <= len(var) <= 4]
    data_var = [var for var in data_var if var not in ['crs_latlon', 'reftime', 'leadtime', 'rotated_pole', 'polar_stereo']][0]
    cartopy_globe = getattr(ccrs, 'Globe')(**dataset.attrs['cartopy_crs_globe_params'])
    cartopy_crs_projection_params = dataset.attrs['cartopy_crs_projection_params'].copy()
    cartopy_projection = cartopy_crs_projection_params.pop('cartopy_projection')
    crs_plot = getattr(ccrs, cartopy_projection)(**cartopy_crs_projection_params, globe=cartopy_globe)
    project_bool = not isinstance(crs_plot, ccrs.PlateCarree)
    global_extent_bool = isinstance(crs_plot, ccrs.PlateCarree)
    coastline_projected = (gf.coastline * gf.borders * gf.ocean * gf.lakes * gf.rivers).opts(projection=crs_plot)
    da = dataset[data_var]
    plot = coastline_projected * da.hvplot.quadmesh(rasterize=True, data_aspect=1, frame_height=550, cmap='viridis', crs=crs_plot, projection=crs_plot, project=project_bool, global_extent=global_extent_bool, geo=True).opts(alpha=0.8)

    return plot
