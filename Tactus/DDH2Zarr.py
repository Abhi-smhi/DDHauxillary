import glob
import time
import subprocess
import numpy as np
import xarray as xr
import dask
import dask.bag as db
import dask.array as da
import pandas as pd
from dask.distributed import Client, get_client, progress


# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
# └────────────────────────────────────────────────────────────────────────────┘
INPUT_DIR = "/ec/res4/scratch/swe7088/deode/ddh_mat_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/"
INPUT_DIR = "/ec/res4/scratch/swe7088/deode/ddh_mat2_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/"
PATTERN         = "DHFDLDEOD+*s"
NWORKER         = 32
MEMLIMIT        = '32GB'
BATCH_SIZE      = 10
ARTICLE_FILE    = 'ddh_article_list3'
OUTPUT_FILE     = "/perm/swe7088/dask_test_out2D.zarr"
GEOM_FILE        = 'geom_ddh.dat'

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG ENDS HERE                            │
# └────────────────────────────────────────────────────────────────────────────┘


def read_file_T(fasta_path):
    result = subprocess.run([ "ddhr", '-b', '-es',
        str(fasta_path)
        ],
        capture_output=True,
        text=True,
        check=True)

    result = result.stdout
    start = result[0:10] + 'T' + result[11:16]
    D = np.fromstring(result[17:], sep='\n')[0] # Duration in s
    time = np.datetime64(start, 's') + int(D)
    return time

def read_DDH_meta(file):
    '''
    '''
    import pandas  as pd
    domain = pd.read_csv(file)

    jlon = domain['jlon'].values
    jgl = domain['jgl'].values

    lon = domain['lons'].values
    lat = domain['lats'].values

    jlon_un = np.unique(jlon)
    jgl_un = np.unique(jgl)

    lons_grid = lon.reshape(jlon_un.size, jgl_un.size, order='F')
    lats_grid = lat.reshape(jlon_un.size, jgl_un.size, order='F')

    n_levels = 90 # Hardcoded for now

    return n_levels, jlon_un, jgl_un, lons_grid, lats_grid, jlon, jgl

def read_DDH_data(path, articles, n_levels, n_lon, n_lat):
    data = np.zeros([len(articles), n_levels, n_lon, n_lat])

    for i, article in enumerate(articles):
        result_article = subprocess.run([
           "lfac",
           str(path),
           article
        ],
        capture_output=True,
        text=True,
        check=True)
        data[i] = (np.fromstring(result_article.stdout, sep ='\n')
                .reshape(n_levels, n_lon, n_lat, order='F'))
    return data

def read_batch(files, articles, n_levels, n_lon, n_lat):
    data_batch = []
    for f in files:
        data = read_DDH_data(f, articles, n_levels, n_lon, n_lat)
        data_batch.append(data)

    return np.stack(data_batch, axis=0)



if __name__ == "__main__":

    file_list = sorted(glob.glob(INPUT_DIR + PATTERN))
    n_times = len(file_list)
    n_levels, jlon_un, jgl_un, lons_grid, lats_grid, jlon, jgl = read_DDH_meta(GEOM_FILE)

    n_domains = jlon_un.size * jgl_un.size
    n_lon = jlon_un.size
    n_lat = jgl_un.size
    n_times = len(file_list)

    print(f'\n\n-------------------------INFO------------------------------')
    print(f'Found {n_times} DDH files')
    print(f'DDH files contain {n_domains} domains')
    print(f'Grid: {n_levels} levels, {n_lon} nlon, {n_lat} nlats')
    print(f'-----------------------------------------------------------')


    # -- ddh articles to process
    articles = pd.read_table(ARTICLE_FILE).values.flatten()

    with Client(n_workers=NWORKER, threads_per_worker=1, memory_limit=MEMLIMIT) as client:
        print(f'\n\nStarted Dask client {client}')
        print(f'Dashboard link {client.dashboard_link}\n')

        files_bag = db.from_sequence(file_list)
        proc_times = files_bag.map(read_file_T)
        persist = client.persist(proc_times)

        print(f'[Dask] Computing DDH validities')
        progress(persist)
        actual_times = persist.compute()


        print('Reading DDH data')
        articles_future = client.scatter(articles, broadcast=True)
        file_batches = [file_list[i:i+BATCH_SIZE] for i in range(0, len(file_list), BATCH_SIZE)]

        lazy_batches = []

        for batch in file_batches:
            current_shape = (len(batch), len(articles), n_levels, n_lon, n_lat)
            d_part  = dask.delayed(read_batch)(batch, articles_future, n_levels, n_lon, n_lat)
            b_array = da.from_delayed(d_part, shape=current_shape, dtype='float64')
            lazy_batches.append(b_array)

        print(f'[Dask] Reading data in {len(file_batches)} batches')
        da_stack = da.concatenate(lazy_batches, axis=0)

        print(f'[xarray] Creating data array')
        d_array = xr.DataArray(
                data=da_stack,
                dims = ['time', 'article', 'level', 'jlon', 'jgl'],
                coords = {
                    'time': actual_times,
                    'article': articles,
                    'level': np.arange(n_levels) + 1,
                    'jlon': jlon_un,
                    'jgl': jgl_un,
                    'longitude': (['jlon', 'jgl'], lons_grid),
                    'latitude': (['jlon', 'jgl'], lats_grid)
                    }

                )
        print(f'[xarray] Datasets for each article')
        ds = d_array.to_dataset(dim='article')

        print(f'[Dask] Writing to {OUTPUT_FILE}')
        write_job = client.persist(ds.to_zarr(OUTPUT_FILE, compute=False, mode='w'))
        t_start = time.time()
        progress(write_job)
        t_end = time.time()
        duration = int(t_end - t_start)
        print(f'[Dask] Writing completed in {duration} seconds')
        print(f'[Dask] Done, closing dask client')

