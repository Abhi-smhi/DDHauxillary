import epygram as epg
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
import pandas as pd

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
# └────────────────────────────────────────────────────────────────────────────┘


# Bounding box tool: https://tools.mofei.life/bbox
LONMIN = 2.310136054323812
LONMAX = 2.3537636353760402
LATMIN = 48.84862807230194
LATMAX = 48.87445809543949
INPUT_FILE = '/scratch/swe7088/deode/LEO_test_osm_pgd_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/GRIBPFDEOD+0026h00m00s'
OUTPUT_CONFIG = 'ddh_modif.toml'
GEOM_FILE  = 'geom_ddh.dat'
PLOT_DOMAIN = False

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STOPS HERE                           │
# └────────────────────────────────────────────────────────────────────────────┘


print(f"\n{'-' * 40} SETTINGS {'-' * 40}")
print(f"  INFILE  :: {INPUT_FILE}")
print(f"  OUT     :: {OUTPUT_CONFIG}")
print(f"  AREA    :: {LONMIN}°W, {LONMAX}°E / {LATMIN}°S, {LATMAX}°N")
print(f"{'-' * 90}")

def generate_bdeddh_entries(jlons, jgls):
    """
    Modified from Marvin K (Met Norway) (https://github.com/MetNoMarvinK/DDHauxillary.git)

    Generates entries for NAMDDH for all grid points in input array

    Parameters:
    jlons: 1d array dim(n_points)
    jgls: 1d array dim(n_points)


    Returns:
    L = string to convert to tacuts modification file

    """
    L = ''
    for c1, lonlat in enumerate(zip(jlons.flatten(),jgls.flatten())):
        jlon = lonlat[0]
        jgl  = lonlat[1]

        entry = (
                f'"BDEDDH(1,{c1+1})" = 1.0\n' +
                f'"BDEDDH(2,{c1+1})" = 1.0\n' +
                f'"BDEDDH(3,{c1+1})" = {jlon}\n'+
                f'"BDEDDH(4,{c1+1})" = {jgl}\n'
            )
        L += entry
    return ''.join(L)

if __name__ == "__main__":

    epg.init_env()

    print('Reading GRIB/LFA file')
    with  epg.formats.resource(INPUT_FILE, 'r') as res_epg:
        print('Reading field (CLSTEMPERATURE/2 metre temperature)')
        if res_epg.format == 'FA':
            field = res_epg.readfield('S010WIND.U.PHYS')
            field.sp2gp()
        elif res_epg.format == 'GRIB':
            field = res_epg.readfield({'name':'2 metre temperature'})

    print(f'Extracting zoom [{LONMIN}, {LONMAX}, {LATMIN}, {LATMAX}]')
    zoom = dict(lonmin=LONMIN, lonmax=LONMAX, latmin=LATMIN, latmax=LATMAX)
    field_zoom = field.extract_zoom(zoom)
#    lons, lats = field_zoom.field_zoom.geo
    lons, lats = field_zoom.geometry.get_lonlat_grid()

    print(f'Found {lons.size} DDH domains')

    # Find the exact jlon and jgl for this
    imin, jmin = field.geometry.ll2ij(LONMIN, LATMIN)
    imax, jmax = field.geometry.ll2ij(LONMAX, LATMAX)

    imin = max(int(np.ceil(imin)), 0)
    imax = min(int(np.floor(imax)), field.geometry.dimensions['X'] - 1)
    jmin = max(int(np.ceil(jmin)), 0)
    jmax = min(int(np.floor(jmax)), field.geometry.dimensions['Y'] - 1)

    jlon = np.arange(start=imin, stop=imax+1)
    jgl  = np.arange(start=jmin, stop=jmax+1)
    jlon_grid, jgl_grid = np.meshgrid(jlon, jgl, indexing = 'ij')

    domain_data = {
            'jlon': jlon_grid.flatten(order='F'),
            'jgl': jgl_grid.flatten(order='F'),
            'lons': lons,
            'lats': lats}


    print(f'Writing geometry data to {GEOM_FILE}')
    pd.DataFrame(domain_data).to_csv(GEOM_FILE)

    print(f'Writing config file {OUTPUT_CONFIG}')
    wstring = generate_bdeddh_entries(jlon, jgl)

    with open(OUTPUT_CONFIG, 'w') as file:
        file.write('[namelist_update.master.forecast.NAMDDH]\n' + wstring)

    if PLOT_DOMAIN:
        print('plottin domain: dom.png')

        fig =  plt.figure(figsize=(14,7),tight_layout=True)
        ax1 = fig.add_subplot(1,2,1, projection=field.geometry.default_cartopy_CRS())
        ax2 = fig.add_subplot(1,2,2, projection=field_zoom.geometry.default_cartopy_CRS())

        field.cartoplot(epygram_departments=True, colormap='RdYlBu_r', fig=fig,
                ax=ax1)
        field_zoom.cartoplot(epygram_departments=True, colormap='RdYlBu_r',
                fig=fig, ax=ax2)

        # -- box showing bounding box
        corner = (zoom['lonmin'], zoom['latmin'])
        box_width  = zoom['lonmax']-zoom['lonmin']
        box_height = zoom['latmax']-zoom['latmin']
        patch = Rectangle(corner, box_width, box_height, fill=False,
                transform=ccrs.PlateCarree(), edgecolor='green', linewidth=4.)
        ax1.add_patch(patch)
        ax1.set_title('T2m')
        ax2.set_title('T2m zoomed')
        fig.suptitle('T2m with selected area', fontsize=16)

        fig.savefig('dom.png')
