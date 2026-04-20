import epygram as epg
import os
import sys
import yaml
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt
import cartopy.crs as ccrs


def load_config(config_path):
    """Loads and validates the YAML configuration file."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)

    with open(config_path, 'r') as file:
        # 1. Read the raw text from the file
        raw_config = file.read()

        # 2. Expand environment variables in the string
        expanded_config = os.path.expandvars(raw_config)
        try:
            config = yaml.safe_load(expanded_config)
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML: {exc}")
            sys.exit(1)

    return config

def parse_config():
    """
    Parses config.yaml

    param: None

    returns:

    list [lonmin, lonmax, latmin, latmax, input_file, output_config]

    """


    config_file = "config.yaml"
    cfg = load_config(config_file)

    # Simple validation
    required_fields = ['lonmin', 'lonmax', 'latmin', 'latmax', 'input_file', 'output_config']
    for field in required_fields:
        if not cfg.get(field):
            print(f"Error: Missing required field '{field}' in {config_file}")
            sys.exit(1)

    lonmin = cfg.get('lonmin')
    lonmax = cfg.get('lonmax')
    latmin = cfg.get('latmin')
    latmax = cfg.get('latmax')
    input_file = cfg.get('input_file')
    output_config = cfg.get('output_config')
    plot_domain = cfg.get('plot_domain')


    print("\n\n--- Configuration Loaded ---")
    print(f"File (GRIB/LFA): {input_file}")
    print(f"Output config:  {output_config}")
    print(f"Bounding box [lonmin, lonmax, latmin, latmax]:  [{lonmin}, {lonmax}, {latmin}, {latmax}]")
    print("----------------------------")

    return [lonmin, lonmax, latmin, latmax, input_file, output_config, plot_domain]


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
    lonmin, lonmax, latmin, latmax, input_file, output_config, plot_domain = parse_config()


    print('Reading GRIB/LFA file')
    res_epg = epg.formats.resource(input_file, 'r')


    print('Reading field (CLSTEMPERATURE/2 metre temperature)')
    if res_epg.format == 'FA':
        field = res_epg.readfield('CLSTEMPERATURE')
    elif res_epg.format == 'GRIB':
        field = res_epg.readfield({'name':'2 metre temperature'})

    print('Extracting zoom [{lonmin}, {lonmax}, {latmin}, {latmax}]')
    zoom = dict(lonmin=lonmin, lonmax=lonmax, latmin=latmin, latmax=latmax)
    field_zoom = field.extract_zoom(zoom)
    lons, lats = field_zoom.geometry.get_lonlat_grid()

    print(f'Writing config file {output_config}')
    wstring = generate_bdeddh_entries(lons, lats)

    with open(output_config, 'w') as file:
        file.write('[namelist_update.master.forecast.NAMDDH]\n' + wstring)

    if plot_domain:
        fig =  plt.figure(figsize=(7,7))
        ax = fig.add_subplot(1,1,1, projection=field.geometry.default_cartopy_CRS())
        field.cartoplot(epygram_departments=True, colormap='RdYlBu_r', fig = fig, ax = ax)

        corner = (zoom['lonmin'], zoom['latmin'])
        box_width  = zoom['lonmax']-zoom['lonmin']
        box_height = zoom['latmax']-zoom['latmin']
        patch = Rectangle(corner, box_width, box_height, fill=False,
                transform=ccrs.PlateCarree(), edgecolor='green', linewidth=4.)
        ax.add_patch(patch)
        ax.set_title('T2m with selected area')
        fig.savefig('dom.png')



