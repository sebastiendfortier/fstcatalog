# -*- coding: utf-8 -*-
import os
import warnings
from multiprocessing import Pool
from pathlib import Path

import fstd2nc
import fstpy
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
import cmcdict
from intake.catalog.local import LocalCatalogEntry, Catalog
import xarray as xr

# rmn.fstopt('MSGLVL', 'ERRORS')

# warnings.simplefilter('ignore')

# fstd2nc.stdout.streams = ('errors',)

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
            df = pd.concat(results, ignore_index=True)

        self._df = FstCatalog._get_descriptions(df)
        self._df = self._df.applymap(str)
        agg_func = {col: lambda x: ','.join(
            x.unique()) for col in self._df.columns if col not in grouping}
        self._df = self._df.groupby(grouping, as_index=False).agg(agg_func)
        # column_types = {col: 'str' for col in self._df.columns}
        # self._df = self._df.astype(column_types)
        #     groups = [(group.reset_index(drop=True), self._by_var)
        #               for _, group in df.groupby(grouping)]
        #     results = p.starmap(FstCatalog._aggregate_dataframe_columns, groups)
        #     self._df = pd.concat(results, ignore_index=True)
        #     # display(df)
        #     # derived_values = p.map(FstCatalog._get_record_crs_info, [row for _, row in df.iterrows()])

        # # df[['crs_cf_name', 'WKT']] = pd.DataFrame(derived_values)
        self._set_description()
        self._set_source()
        self._rename_columns()
        self._df['driver'] = 'fst'
        self._df['forecast_axis'] = True
        self._get_filter()
        self._select_columns()
        # return self

    @staticmethod
    def _get_descriptions(df):
        descriptions_df = pd.DataFrame([cmcdict.get_metvar_metadata(nomvar) for nomvar in df.nomvar.unique(
        ).tolist() if cmcdict.get_metvar_metadata(nomvar) is not None])
        df = df.merge(descriptions_df[[
                      'nomvar', 'description_short_en', 'units']], on='nomvar', how='left')
        return df

    def _set_description(self):
        self._df['description'] = self._df.apply(
            lambda row: str(row['description_short_en']) + ' ' + str(row['units']), axis=1)

    def _set_source(self):
        self._df['source'] = self._df.apply(
            lambda row: row['nomvar'].replace(',', '_') + '_' + row['etiket'] + '_' + row['level_unit'],  axis=1)
        self._number_dups()

    def _rename_columns(self):
        self._df = self._df.rename(
            columns={'path': 'urlpath', 'nomvar': 'vars'})

    def _get_filter(self):
        self._df['filter'] = self._df.apply(
            FstCatalog._get_fstd2nc_search_filter, axis=1)

    def _select_columns(self):
        self._df = self._df[['source', 'description', 'driver',
                             'urlpath', 'vars', 'filter', 'forecast_axis']]

    def _number_dups(self):
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

    def to_yaml(self, path):
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
        data_sources = {}
        data_dict = self._df.to_dict(orient='records')
        for entry in data_dict:
            entry_args = {
                'urlpath': entry['urlpath'].split(','),
                'vars': list(entry['vars']),
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
    def combine_catalogs(catalogs: list):
        from intake.catalog import Catalog

        # Assuming you have catalogs named 'cat1', 'cat2', and 'cat3'
        combined_cat = Catalog(name='combined')

        # Combine the catalogs by adding their entries
        for catalog in catalogs:
            for entry_name, entry in catalog.items():
                combined_cat[entry_name] = entry

        return combined_cat
    # def to_intake(self):
    #     #         mycat = Catalog.from_dict({'source1': LocalCatalogEntry(name, description, driver, args=...),
    #     #     ...
    #     # })
    #     data_dict = self._df.to_dict(orient='records')
    #     catalog_entries = {}

    #     for entry in data_dict:
    #         entry_dict = {}
    #         entry_dict['description'] = entry['description']
    #         entry_dict['driver'] = entry['driver']

    #         args = {}
    #         args['urlpath'] = entry['urlpath'].split(',')
    #         args['vars'] = entry['vars']
    #         args['filter'] = entry['filter']
    #         args['forecast_axis'] = entry['forecast_axis']

    #         entry_dict["args"] = args

    #         catalog_entries[entry['source']] = entry_dict

    #     return intake.Catalog(catalog_entries, name='catalog')


# # Create a catalog using the dictionary of catalog entries
# catalog = intake.Catalog(catalog_entries)

    # def voir_view(self) -> pd.DataFrame:
    #     """Returns the a subset of columns of the dataframe records that correspond to a 'voir'

    #     :return: subset of columns of the dataframe records that correspond to a 'voir'
    #     :rtype: pd.DataFrame
    #     """
    #     return self._df[['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
    #                      'ip3', 'grtyp', 'ig1', 'ig2', 'ig3',
    #                      'ig4']]

    # def advanced_view(self) -> pd.DataFrame:
    #     """Returns the a subset of columns of the dataframe records that have decoded values

    #     :return: subset of columns of the dataframe records that have decoded values
    #     :rtype: pd.DataFrame
    #     """
    #     return self._df[['nomvar', 'description_short_en', 'units', 'date_of_observation', 'date_of_validity', 'level', 'level_unit', 'interval']]

    # def get_dataset(self, index: int) -> Dataset:
    #     """gets a selected record from the catalog as a xarray dataset

    #     :param index: catalog index for a row
    #     :type index: int
    #     :raises FstCatalogError: Index out of range
    #     :return: xarray dataset of the selected record
    #     :rtype: xarray.Dataset
    #     """
    #     if index in self._df.index:
    #         ds = fstd2nc.Buffer(self._df.loc[index].path.split(','), vars=[f'{self._df.loc[index].nomvar}'], filter=FstCatalog._get_fstd2nc_search_filter(
    #             self._df.loc[index]), rpnstd_metadata=True, opdict=True).to_xarray()
    #         ds = ds.assign(WKT=self._df.loc[index].WKT)
    #         ds = ds.assign(proj4=pcc.from_wkt(
    #             self._df.loc[index].WKT).to_proj4())
    #         return ds
    #     else:
    #         raise FstCatalogError("Invalid record index")

    # def get_dataset_with_crs_info(self, index: int) -> Tuple[Dataset, str, str]:
    #     """gets a selected record accompanied with from the catalog as a xarray dataset

    #     :param index: catalog index for a row
    #     :type index: int
    #     :raises FstCatalogError: Index out of range
    #     :return: xarray dataset of the selected record, crs_cf_name and proj4 string
    #     :rtype: Tuple[Dataset, str, str]
    #     """
    #     ds = self.get_dataset(index)
    #     crs_cf_name = self._df.loc[index].crs_cf_name
    #     proj4 = pcc.from_wkt(self._df.loc[index].WKT).to_proj4()
    #     return ds, crs_cf_name, proj4


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

    #     'nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
    #    'ip3', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'grid', 'path',
    #    'date_of_observation', 'date_of_validity', 'level', 'level_unit',
    #    'interval', 'vctype', 'filter', 'description_short_en', 'units'

        return df

    @staticmethod
    def _aggregate_dataframe_columns(df: pd.DataFrame, by_var=True) -> pd.DataFrame:
        """Aggregates specific columns in to comma separated strings

        :param df: Catalog
        :type df: pd.DataFrame
        :return: DataFrame with aggregated columns
        :rtype: pd.DataFrame
        """
        column_types = {col: 'str' for col in df.columns}
        df.astype(column_types)
        newdf = pd.DataFrame([df.iloc[0]])
        newdf.reset_index(drop=True, inplace=True)
        # if not by_var:
        #     newdf.at[0, 'nomvar'] = ','.join(
        #         np.sort(df.nomvar.unique()).astype(str).tolist())
        newdf.at[0, 'level'] = ','.join(
            np.sort(df.level.unique()).astype(str).tolist())
        newdf.at[0, 'ip1'] = ','.join(
            np.sort(df.ip1.unique()).astype(str).tolist())
        newdf.at[0, 'ip2'] = ','.join(
            np.sort(df.ip2.unique()).astype(str).tolist())
        newdf.at[0, 'ip3'] = ','.join(
            np.sort(df.ip3.unique()).astype(str).tolist())
        newdf.at[0, 'date_of_observation'] = ','.join(
            np.sort(df.date_of_observation.unique()).astype(str).tolist())
        newdf.at[0, 'date_of_validity'] = ','.join(
            np.sort(df.date_of_validity.unique()).astype(str).tolist())
        newdf.at[0, 'path'] = ','.join(np.sort(df.path.unique()).tolist())
        newdf.at[0, 'interval'] = ','.join(
            np.sort(df.interval.unique()).astype(str).tolist())
        return newdf

    # @staticmethod
    # def _get_record_crs_info(row) -> pd.DataFrame:
    #     """for a catalog row, gets the WKT and cf crs name

    #     :param df: Catalog
    #     :type df: pd.DataFrame
    #     :return: Catalog with the crs_cf_name and WKT columns added
    #     :rtype: pd.DataFrame
    #     """
    #     nomvar = row.nomvar
    #     first_file = row.path.split(',')[0]

    #     ds = fstd2nc.Buffer(
    #         first_file,
    #         vars=[nomvar],
    #         filter=FstCatalog._get_fstd2nc_search_filter(row)
    #     ).to_xarray()

    #     crs_cf_name = ''
    #     wkt = ''
    #     if nomvar[0].isdigit():
    #         nomvar = '_'.join(['', nomvar])
    #     if len(ds):
    #         grid_mapping = ds[nomvar].attrs.get('grid_mapping')
    #         crs_cf_name = grid_mapping
    #         wkt = pcc.from_cf(ds[grid_mapping].attrs).to_wkt()

    #     result = pd.Series([crs_cf_name, wkt])

    #     return result

    # def get_hvplot(self, index: int):
    #     if index in self._df.index:
    #         crs_in = PlateCarree()
    #         project_in = False
    #         ds, grid_name, proj4_in = self.get_dataset_with_crs_info(index)
    #         if grid_name == 'rotated_pole':
    #             crs_in = proj4_in
    #             project_in = True

    #         return ds.hvplot.quadmesh(
    #             x='lon',
    #             y='lat',
    #             rasterize=True,
    #             data_aspect=1,
    #             frame_height=550,
    #             cmap='jet',
    #             crs=crs_in,
    #             projection=PlateCarree(),
    #             project=project_in,
    #             geo=True,
    #             coastline=True,
    #             widget_location='bottom'
    #         )
    #     else:
    #         raise FstCatalogError(f"Invalid record index: {index}")

    # @staticmethod
    # def _get_fstd2nc_search_filter(row) -> list:
    #     filter = [
    #         f"typvar=='{row.typvar}'",
    #         f"etiket=={row.etiket.ljust(12).encode('utf8')!r}",
    #         f"np.isin(ip1, {list(map(int,str(row.ip1).split(',')))})",
    #         f"np.isin(ip2, {list(map(int,str(row.ip2).split(',')))})",
    #         f"np.isin(ip3, {list(map(int,str(row.ip3).split(',')))})",
    #         f'ni=={row.ni}',
    #         f'nj=={row.nj}',
    #         f'nk=={row.nk}',
    #         f"grtyp=='{row.grtyp}'"
    #     ]
    #     # filter = json.dumps(filter)
    #     # display(filter)
    #     return filter

    @staticmethod
    def _get_fstd2nc_search_filter(row) -> list:
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
        # filter = json.dumps(filter)
        # display(filter)
        return filter

    @staticmethod
    def get_hvplot(ds: xr.Dataset, data_var: str, cmap: str = 'jet', alpha: float = 0.5):
        from cartopy.crs import PlateCarree
        import cartopy
        import geoviews.feature as gf
        import hvplot.xarray
        if ds is None:
            print("Error: unable to create hvplot due to missing dataset")
            return None
        cartopy_globe = getattr(cartopy.crs, 'Globe')(
            **ds.attrs['cartopy_crs_globe_params'])
        cartopy_crs_projection_params = ds.attrs['cartopy_crs_projection_params'].copy(
        )
        cartopy_projection = cartopy_crs_projection_params.pop(
            'cartopy_projection')
        crs_plot = getattr(cartopy.crs, cartopy_projection)(
            **cartopy_crs_projection_params, globe=cartopy_globe)
        project_bool = not isinstance(crs_plot, PlateCarree)
        global_extent_bool = isinstance(crs_plot, PlateCarree)
        coastline_projected = (gf.coastline * gf.borders * gf.ocean *
                               gf.lakes * gf.rivers).opts(projection=crs_plot)
        return coastline_projected * ds[data_var].hvplot.quadmesh(rasterize=True, data_aspect=1, frame_height=550, cmap=cmap, crs=crs_plot, projection=crs_plot, project=project_bool, global_extent=global_extent_bool, geo=True).opts(alpha=alpha)
