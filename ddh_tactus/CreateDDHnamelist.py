import epygram as epg
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STARTS HERE                          │
# └────────────────────────────────────────────────────────────────────────────┘


# Bounding box tool: https://tools.mofei.life/bbox
lonmin = 2.188582
lonmax = 2.493856
latmin = 48.802005
latmax = 48.922987
input_file = '/scratch/swe7088/deode/LEO_test_osm_pgd_CY49t2_HARMONIE_AROME_LES_input_Paris_200m_linear_20230820/archive/2023/08/20/12/mbr000/GRIBPFDEOD+0026h00m00s'
output_config = 'ddh_modif.toml'
plot_domain = True


# ┌────────────────────────────────────────────────────────────────────────────┐
# │                           USER CONFIG STOPS HERE                           │
# └────────────────────────────────────────────────────────────────────────────┘


print(f"\n--- SETTINGS {'-' * 30}")
print(f"  ID  :: {input_file}")
print(f"  OUT :: {output_config}")
print(f"  AREA:: {lonmin}°W, {lonmax}°E / {latmin}°S, {latmax}°N")
print(f"{'-' * 43}")

def generate_bdeddh_entries(lons, lats):
    """
    Modified from Marvin K (Met Norway) (https://github.com/MetNoMarvinK/DDHauxillary.git)

    Generates entries for NAMDDH for all grid points in input array

    Parameters:
    lons: 1d array dim(n_points)
    lats: 1d array dim(n_points)


    Returns:
    L = string to convert to tacuts modification file

    """
    L = ''
    for c1, lonlat in enumerate(zip(lons.flatten(),lats.flatten())):
        lon = lonlat[0]
        lat = lonlat[1]

        entry = (
                f'"BDEDDH(1,{c1+1})" = 4.0\n' +
                f'"BDEDDH(2,{c1+1})" = 1.0\n' +
                f'"BDEDDH(3,{c1+1})" = {lon}\n'+
                f'"BDEDDH(4,{c1+1})" = {lat}\n'
            )
        L += entry
    return ''.join(L)


if __name__ == "__main__":

    epg.init_env()

    print('Reading GRIB/LFA file')
    with  epg.formats.resource(input_file, 'r') as res_epg:
        print('Reading field (CLSTEMPERATURE/2 metre temperature)')
        if res_epg.format == 'FA':
            field = res_epg.readfield('CLSTEMPERATURE')
        elif res_epg.format == 'GRIB':
            field = res_epg.readfield({'name':'2 metre temperature'})

    print('Extracting zoom [{lonmin}, {lonmax}, {latmin}, {latmax}]')
    zoom = dict(lonmin=lonmin, lonmax=lonmax, latmin=latmin, latmax=latmax)
    field_zoom = field.extract_zoom(zoom)
    lons, lats = field_zoom.geometry.get_lonlat_grid()

    print(f'Found {lons.size} DDH domains')

    print(f'Writing config file {output_config}')
    wstring = generate_bdeddh_entries(lons, lats)

    with open(output_config, 'w') as file:
        file.write('[namelist_update.master.forecast.NAMDDH]\n' + wstring)

    if plot_domain:
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
