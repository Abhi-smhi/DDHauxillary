import epygram as epg
import numpy as np
import glob
import pandas as pd
import epygram as epg
import xarray as xr
import dask.array as da
import dask.bag as db
import dask
from dask.distributed import Client, get_client

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
# └────────────────────────────────────────────────────────────────────────────┘
INPUT_DIR = "/ec/res4/scratch/swe7088/deode/ddh_mat_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/"
#ZARR_PATH = "/hpcperm/swe7088/output_data.zarr"
ZARR_PATH = "test.zarr"
PATTERN = "DHFDLDEOD+*s"
CHUNK_SIZE = 100    # Number of files to process before flushing to Zarr
ARTICLE_FILE = 'ddh_article_list3'

NWORKER = 16    # no. of workers for dask
MEMLIMIT = '16GB'   # Gb  for dask

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STOPS HERE                           │
# └────────────────────────────────────────────────────────────────────────────┘

# --- glob DDH files in INPUT_DIR
file_list = glob.glob(INPUT_DIR + PATTERN)
file_list.sort()
file_list = file_list
file1 = file_list[0]


# --- Read first file to extract grid data
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

# -- ddh articles to process
articles = pd.read_table(ARTICLE_FILE).values.flatten()


# -- mapping dictionary for indices
R = jlon_un.size
map_ij = {}
for i in range(len(jgl)):
    map_ij[i]=(i%R, int(i/R))

def read_time(file_path):
    """
    """
    with epg.formats.resource(file_path, openmode='r', fmt='DDHLFA') as res:
        validity = np.datetime64(res.validity.get())
        return validity

def read_articles(file_path):
    """
    param:
    file_path: str
        file path to DDH file
    return:

    data:   ndarray[article, level, jlon, jgl]
        DDH articles in ARTICLE_FILE, gridded to lon and lat indices for all
        model levels


    """
    data = np.zeros([len(articles), n_levels, jlon_un.size, jgl_un.size])
    print(f'Reading file {file_path}')
    with epg.formats.resource(file_path, openmode='r', fmt='DDHLFA') as res:
        for i, article in enumerate(articles):
            field = res.readfield(article)
            for j, domain in enumerate(field):
                ix, jx = map_ij[j]
                data[i,:, ix, jx] = domain.data
    return data


if __name__ == "__main__":
    epg.init_env()

    # 1. Setup Dask Cluster
    try:
        client = get_client()
        print(f"Using existing client: {client}")
    except ValueError:
        client = Client(n_workers=NWORKER, threads_per_worker=1, memory_limit=MEMLIMIT)
        print(f"Created new client: {client}")

    read_time = dask.delayed(read_time, pure=True)
    read_article_d = dask.delayed(read_articles, pure=True)

    print(f'\n\tLazy reading DDH validites')
    lazy_times = [read_time(file) for file in file_list]
    times = dask.compute(*lazy_times)

    shape = (len(articles), n_levels, jlon_un.size, jgl_un.size)
    dtype = 'float64'


    print(f'\n\tLazy reading DDH articles')
    lazy_data = [read_article_d(file)
            for file in file_list]

    da_data = [da.from_delayed(dfile, shape=shape, dtype=dtype)
            for dfile in lazy_data]

    stacked = da.stack(da_data, axis=0)

    # -- Assmeble dataset without computing
    data_var = {}
    for i, article in enumerate(articles):
        data_var[article] = (['time', 'levels', 'jlon','jgl'], stacked[:,i,:,:,:])

    print(f'\n\tAssembling dataset')
    ds = xr.Dataset(
            data_vars = data_var,
            coords={
            'time': list(times),
            'level': np.arange(n_levels) + 1,
            'jgl': jgl_un,
            'jlon': jlon_un,
            'lat': (['jlon', 'jgl'], lats_grid),
            'lon': (['jlon', 'jgl'], lons_grid) }
            )

    #s = ds.chunks(time=100)

    print(f'\n\tWriting to zarr..')
    ds.to_zarr(ZARR_PATH, mode='w', consolidated=True)

    print(f'\n\tZarr file {ZARR_PATH} written')
    print(f'\n\t DONE! Closing client..\n\n')

    client.close()
