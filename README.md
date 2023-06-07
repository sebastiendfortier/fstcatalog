# Introduction

## What is it?

This module provides a class for cataloging fst files into an intake
catalog.

## fstcatalog philosophy

Provide a structured catalog of records that belong together according
to their grid and generation process.

# Requirements

-   cartopy\>=0.21.0
-   cmcdict @ git+https://gitlab.science.gc.ca/CMDS/cmcdict.git
-   fstd2nc @ git+https://github.com/neishm/fstd2nc.git
-   fstpy @ git+https://gitlab.science.gc.ca/CMDS/fstpy.git
-   geoviews\>=1.9.6
-   hvplot\>=0.8.2
-   intake\>=0.6.8
-   numpy\>=1.23.5
-   pandas\>=1.5.3
-   pyproj\>=3.3.1
-   xarray\>=2023.2.0

# Installation

## Gitlab

Use the git repository package:

    python3 -m pip install git+http://gitlab.science.gc.ca/CMDS/fstcatalog.git

## SSM

    . r.load.dot . /fs/ssm/eccc/cmd/cmds/fstcatalog/latest

### Use fstcatalog

``` shell
# inside your script
>>> import fstcatalog
>>> import datetime
>>> from pathlib import Path
>>> from glob import glob
>>> current_date = datetime.datetime.now()
>>> year_month_day_string = current_date.strftime('%Y%m%d')
>>> filter = '00_01*'
>>> base_path1 = Path('/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta')
>>> files = glob(f'{base_path1}/{year_month_day_string}{filter}')
>>> cat = fstcatalog.FstCatalog(files)
>>> cat.df
               source                                        description   driver                                            urlpath vars                                             filter  forecast_axis
0     5P_G1_8_1_0N_mb          Fraction of grid covered by snow fraction  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   5P  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
1     AB_G1_8_1_0N_mb  Incoming visible accumulated flux (accumulated...  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   AB  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
2    ABE_G1_8_1_0N_mb                    Available bouyant energy J kg⁻¹  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...  ABE  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
3     AD_G1_8_1_0N_mb  Incoming I.R. accumulated flux (accumulated FI...  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   AD  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
4     AE_G1_8_1_0N_mb       Total Accumulated Stratiform Precipitation m  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   AE  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
..                ...                                                ...      ...                                                ...  ...                                                ...            ...
115  WGN_G1_8_1_0N_mb                Instantaneous wind gust minimum m/s  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...  WGN  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
116  WGX_G1_8_1_0N_mb                Instantaneous wind gust maximum m/s  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...  WGX  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
117     WT_G1_8_1_0N_  Fraction of total area occupied by soil on mod...  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   WT  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
118   WW_G1_8_1_0N_sg                               Vertical Motion pa/s  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   WW  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True
119     Z0_G1_8_1_0N_                          Roughness length (Soil) m  fstd2nc  /home/smco500/cmcprod/ppp5/suites/gdps/g1/grid...   Z0  [typvar=='P', etiket=='G1_8_1_0N', np.isin(ip1...           True

[120 rows x 7 columns]
>>> cat.files
['/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_011', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_015_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_012', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_018_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_018', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_011_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_013_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_017_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_019_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_015', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_017', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_016', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_013', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_010', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_014', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_019', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_012_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_010_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_016_extra', '/home/smco500/cmcprod/ppp5/suites/gdps/g1/gridpt.usr/prog/eta/2023060700_014_extra']
>>> icat = cat.to_intake()
>>> list(icat)[:10]
['5P_G1_8_1_0N_mb', 'AB_G1_8_1_0N_mb', 'ABE_G1_8_1_0N_mb', 'AD_G1_8_1_0N_mb', 'AE_G1_8_1_0N_mb', 'AFSD_G1_8_1_0N_mb', 'AFSF_G1_8_1_0N_mb', 'AFSI_G1_8_1_0N_mb', 'AFSV_G1_8_1_0N_mb', 'AG_G1_8_1_0N_mb']
>>> ds = icat.DQST_G1_8_1_0N_.to_dask()
>>> ds
<xarray.Dataset>
Dimensions:       (time: 1, forecast: 3, sfctype: 5, lat: 1251, lon: 1801)
Coordinates:
* time          (time) datetime64[ns] 2023-06-07
* forecast      (forecast) timedelta64[ns] 12:00:00 15:00:00 18:00:00
* lat           (lat) float32 -89.93 -89.78 -89.64 -89.5 ... 89.64 89.78 89.93
* lon           (lon) float32 5.009e-06 0.2 0.4 0.6 ... 359.6 359.8 360.0
Dimensions without coordinates: sfctype
Data variables:
   DQST          (time, forecast, sfctype, lat, lon) float32 dask.array<chunksize=(1, 1, 5, 1251, 1801), meta=np.ndarray>
   surface_type  (sfctype) |S10 ...
   crs_latlon    |S1 ...
Attributes:
   cartopy_crs_projection_params:  {'cartopy_projection': 'PlateCarree'}
   cartopy_crs_globe_params:       {'semimajor_axis': 6370997.0}
>>> fstcatalog.hvplot_cat_entry(ds) 
```

For more examples and information check out the
[documentation](https://web.science.gc.ca/~spst900/fstcatalog/master/index.html)

# Contributing

## Getting the source code

``` bash
git clone git@gitlab.science.gc.ca:cmds/fstcatalog.git
# create a new branch
git checkout -b my_change
# modify the code
# commit your changes
# fetch changes
git fetch
# merge recent master
git merge origin/master
# push your changes
git push my_change
```

Then create a merge request on science\'s gitlab
<https://gitlab.science.gc.ca/CMDS/fstcatalog/merge_requests>

# Acknowledgements

Great thanks to:

-   [Micheal Neish](mailto:Micheal.Neish@canada.ca) for the awsome
    fstd2nc project, great insights on how to develop xarray structure
    from CMC standard files and great functions to work on fst files.
