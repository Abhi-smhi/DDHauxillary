#!/usr/bin/env python
# coding: utf-8

import epygram as epg
import xarray as xr
import numpy as np
import pandas as pd
import glob
import dask.bag as db
from dask.distributed import Client


# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
# └────────────────────────────────────────────────────────────────────────────┘

ddh_files_dir = '/ec/res4/scratch/swe7088/deode/ddh_mat_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/'

n_work = 4          # no. of workers for dask
batch_size = 5000   # split up the zarr processing into batches to conserve RAM
zarr_file = 'DDH_test.zarr'

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STOPS HERE                           │
# └────────────────────────────────────────────────────────────────────────────┘




# --- glob DDH files in dir glob DDH files in dir
file_list = glob.glob(ddh_files_dir + 'DHFDLDEOD+*s')
file_list.sort()
file1 = file_list[0]


# -- Setup up metada from first file
epg.init_env()
with epg.formats.resource(file_list[0], openmode='r', fmt='DDHLFA') as res:
    geom = res.domains['geometry']
    n_levels = len(res.readfield('VPP0')[0].geometry.vcoordinate.levels)

    # Pre-calculate mapping indices
    jgl = np.array([d['jgl'] for d in geom])
    jlon = np.array([d['jlon'] for d in geom])
    lats = np.array([d['lat'].get() for d in geom])
    lons = np.array([d['lon'].get() for d in geom])

# -- grid data
jgl_un = np.unique(jgl)
jlon_un = np.unique(jlon)
lats = np.array(lats)
lons = np.array(lons)


lons_grid = lons.reshape((jlon_un.size, jgl_un.size), order='F')
lats_grid = lats.reshape((jlon_un.size, jgl_un.size), order='F')

articles = pd.read_table('ddh_article_list3').values.flatten()

# -- mapping dictionary for indices
R = jlon_un.size
map_ij = {}
for i in range(len(jgl)):
    map_ij[i]=(i%R, int(i/R))

def proc_one_file(file_path):
    """
    Process single DDH file

    param:
    file_path: (str)

    return:
    ds: xarray dataset
    """
    with epg.formats.resource(file_path, openmode='r', fmt='DDHLFA') as res_loc:
        time_now = res_loc.validity.get()
        file_data = {}
        for i, article in enumerate(articles):
            field_loc = res_loc.readfield(article)
            data_loc = np.zeros([n_levels, jlon_un.size, jgl_un.size])

            for j, domain in enumerate(field_loc):
                # geom = domain.geometry.grid
                ix, jx = map_ij[j]
                data_loc[:, ix, jx] = domain.data
            file_data[article] = (['level', 'jlon', 'jgl'], data_loc)

    # -- Make data set and return
    ds = xr.Dataset(
        data_vars=file_data,
        coords={
            'time': [time_now],
        }
    )
    return ds

def batch_to_zarr(file_list, zarr_path, npart=32, batch_size=5000):
    """
    Processes files in batches, appends to zarr file

    params:

    file_list: str globbed list of files
    zarr_path: str
    batch_size: int

    """

    first_batch = True # To set initial coordinates/dims

    for i in range(0, len(file_list), batch_size):
        batch_files = file_list[i : i + batch_size]
        print(f'Processing batch files {i} to {i + len(batch_files)}')

        b = db.from_sequence(file_list, npartitions=npart)
    datasets = b.map(proc_one_file).compute()

        # Concatenate all datasets along the time dimension
        # Using 'combine_by_coords' or 'concat'
        print("Contatenating..")
        ds = xr.concat(datasets, dim='time', coords='minimal', compat='override', join='exact').sortby('time')
        print("Processing complete.")

        if first_batch:
            ds = ds.assign_coords({'level':('level', np.arange(n_levels) + 1)})
            ds = ds.assign_coords({'jlon':('jlon', jlon_un)})
            ds = ds.assign_coords({'jgl':('jgl',  jgl_un)})
            ds = ds.assign_coords({'lat':(['jlon', 'jgl'], lats_grid )})
            ds = ds.assign_coords({'lon':( ['jlon', 'jgl'], lons_grid )})

            ds.to_zarr(zarr_path, mode='w')
            first_batch = False
        else:
            ds.to_zarr(zarr_path, mode='a', append_dim='true')



if __name__ == "__main__":
    # Start a local cluster (adjust n_workers based on your RAM/CPUs)
    client = Client(n_workers=n_work, threads_per_worker=1)
    batch_to_zarr(zarr_path=zarr_file, npart=n_work, file_list=file_list,
            batch_size=batch_size)

