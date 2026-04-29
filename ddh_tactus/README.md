# Requirements:

0. Tactus (https://github.com/UrbanAIRProject/tactus)
1. epygram (https://github.com/UMR-CNRM/EPyGrAM)
2. cartopy (https://cartopy.readthedocs.io/stable/)
3. matplotlib
4. xarray (https://docs.xarray.dev/en/stable/getting-started-guide/installing.html)
5. numpy


# DDH in Tactus

## Part A: DDH namelist modification

DDH domains can be added to an existing AROME/HARMONIE-AROME experiment by
modifying the experiment configuration (.toml) file. To create a matrix of DDH
domains (point type) within a prescribed box, the output file from a previous
experiment (FA or GRIB file) is read by `CreateDDHnamelist.py` which then
writes (most of) the required modifications to a modification (.toml) file.
This can be read by the Tactus system to create a new experiment with the
required changes.

To use `CreateDDHnamelist.py`, edit the following at the start of the script

1. `INPUT_FILE`: (str) A GRIB/LFA file from an existing experiment.
2. `OUTPUT_CONFIG`: (str) name of the modification file e.g., `ddh_modif.toml`.
3. `LONMIN`, `LONMAX`, `LATMIN`, `LATMAX`: (float) Describes the corners of the box
   from which we wish to extract vertical profiles using DDH..
4. `GEOM_FILE`: (str) name of the output file (needed later) containing
   coordinates and horizontal indices for the DDH domains.
5. `PLOT_DOMAIN`: (bool) outputs an image (dom.png) of the domain showing the
   selected extraction region.

Run the script using

```bash
python CreateDDHnamelists.py
```

## Part B: Modifying the experiment in Tactus

The modified experiment configuration can then be created using tactus as follows:

1. edit `ddh_pre.toml`:
  a. `general.output_settings.ddh_dl`: DDH output frequency. If you want every
(nth) time-step, comment this out the line
`namelist_update.master.forecast.NAMCT0.NDHFDTS` and set
`namelist_update.master.forecast.NAMCT0.NFRDHFD = n`. This step might be
needed if array containing write out steps  DDH exceeds 960 (cf.
`arpifs/module/yomct0.F90` in the IAL source code). This would depend on the
output frequency and the duration of the forecast.

  b. Modify `scheduler.ecfvars.case_prefix`

2. Copy `ddh_modif.toml` and `ddh_pre.toml` to an appropriate place in your
   Tactus working directory. There, you can run
```bash
poetry run deode case my_original_exp.toml ddh_pre.toml ddh_modif.toml
```
which will output the required experiment configuration that has DDH enabled.

## Part C: Converting DDH output to Zarr

DDH ouptuts for all time-steps can be combined to a single Zarr file for
convenient post-processing using the script `DDH2Zarr.py`. The script uses `Dask` to perform the

To use script,
modify the following at the start

1. `INPUT_DIR` : Directory containing all the DDH output.
2. `PATTERN` : glob search pattern for DDH output files e.g., DHFDLDEOD+\*s
3. `NWORKER` : no. of workers to use for Dask, equal to  no.~ of available cores.
4. `BATCH_SIZE`: Size of a batch being processed by a worker at one time, tune
   this to your available RAM and no.~of files that need processing.
5. `GEOM_FILE`: Geometry data file from Part A.
6. `OUTPUT_FILE`: Zarr output.

On ATOS, one could start an interactive job to speed up processing, using up to 32 workers.

```bash
ecinteractive -c32 -m 32G -t 0:30:00
```
Run the script using

```bash
python DDH2Zarr.py
```

TODO:
1. Fluxes using half level output
2. Unit conversion (extensive to intensive)
