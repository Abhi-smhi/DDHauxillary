import glob
import subprocess
import numpy as np
import dask.bag as db
import dask
from dask.distributed import Client, get_client, progress


# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
# └────────────────────────────────────────────────────────────────────────────┘
INPUT_DIR = "/ec/res4/scratch/swe7088/deode/ddh_mat_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/"
PATTERN = "DHFDLDEOD+*s"
NWORKER = 2
MEMLIMIT = '16GB'
# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
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
    Use this only once, epygram is rather slow with dask
    '''
    import epygram as epg
    res =  epg.formats.resource(file_list[0], openmode='r', fmt='DDHLFA')
    geom = res.domains['geometry']
    n_levels = len(res.readfield('VPP0')[0].geometry.vcoordinate.levels)
    res.close()

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

    return n_levels, jlon_un, jgl_un, jlon, jgl, lons_grid, lats_grid

def read_DDH_data(path, articles, n_levels, n_domains):
    data = np.zeros([len(articles), n_levels, n_domains])
    for i, article in enumerate(articles):
        result_article = subprocess.run([
           "lfac",
           str(path),
           article
        ],
        capture_output=True,
        text=True,
        check=True)

        data[i, :,:] = (np.fromstring(result_article.stdout, sep ='\n')
                .reshape(n_domains,n_levels).transpose())
    return data


if __name__ == "__main__":


    file_list = sorted(glob.glob(INPUT_DIR + PATTERN))
    n_levels, jlon_un, jgl_un, jlon, jgl, lons_grid, lats_grid = read_DDH_meta(file_list[0])

    with Client(n_workers=NWORKER, threads_per_worker=1, memory_limit=MEMLIMIT) as client:
        print(f'\n\nStarted Dask client {client}')
        print(f'Dashboard link {client.dashboard_link}\n')

        files_bag = db.from_sequence(file_list)
        proc_times = files_bag.map(read_file_T)
        persist = client.persist(proc_times)

        print(f'[Dask] Computing DDH validities')
        progress(persist)
        actual_times = persist.compute()

