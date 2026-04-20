# Requirements:

0. Tactus (https://github.com/UrbanAIRProject/tactus)
1. epygram (https://github.com/UMR-CNRM/EPyGrAM)
2. cartopy (https://cartopy.readthedocs.io/stable/)
3. matplotlib
4. xarray (https://docs.xarray.dev/en/stable/getting-started-guide/installing.html)
5. numpy


# DDH in Tactus

DDH domains can be added to a AROME/HARMONIE-AROME experiment by modifying the
experiment configuration (.toml) file. To create a matrix of DDH domains (point
type) within a prescribed box, an existing output file from a previous
experiment (FA or GRIB file) is read by `CreateDDHnamelist.py` which then
writes (most of) the required modifications to a modification file which can be
read by the tactus scripts to create a new experiment with the required
changes.

To use `CreateDDHnamelist.py`, edit PART 1 of `config.yaml`:

1. input_file: A GRIB/LFA file from an existing experiment
2. output_config: name of the modification file e.g., `ddh_modif.toml`
3. (lon/lat)(min/max): Describes the corners of the box from which we extract
   data using DDH
4. plot_domain: outputs an image (dom.png) of the domain showing the selected
   extraction region

The modified experiment configuration can then be created using tactus as follows:

1. edit `ddh_pre.toml`:
  a. `general.output_settings.ddh_dl`: DDH output frequency. If you want every
(nth) time-step, comment this out the line
`namelist_update.master.forecast.NAMCT0.NDHFDTS` and set
`namelist_update.master.forecast.NAMCT0.NFRDHFD = n` . This step might be
needed if array containing write out steps  DDH exceeds 960. (cf.
arpifs/module/yomct0.F90 in the IAL source code)
  b. Modifiy `scheduler.ecfvars.case_prefix`

2. Copy `ddh_modif.toml` and `ddh_pre.toml` to an apporpriate place in the tactus folder. There, you can run

`poetry run deode case my_original_exp.toml ddh_pre.toml ddh_modif.toml`

which will output the required experiment configuration which has ddh enabled.

