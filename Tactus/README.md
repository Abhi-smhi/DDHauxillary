# DDH in Tactus

This repository provides tools for adding Diagnostic par Domaines Horizontaux
(DDH) domains to AROME/HARMONIE-AROME experiments within the Tactus system and
post-processing the output. The scripts `CreateDDHnamelist.py` is used to
extract gridpoints within a region of interest which are used to configure the
AROME/HARMONIE-AROME experiement to output vertical profiles (for all lon/lat
points) within this region.  Then, `DDH2Zarr.py` can be used to consolidate the
(potentially large number of) DDH output files into a single multi-dimensional
Zarr chunk which can be read by `xarray` for plotting or further conversion to
NetCDF.


## Prerequisites

Ensure the following dependencies are installed:

* **[Tactus](https://github.com/UrbanAIRProject/tactus)**
* **[epygram](https://github.com/UMR-CNRM/EPyGrAM)**
* **[cartopy](https://cartopy.readthedocs.io/stable/)**
* **matplotlib**
* **[xarray](https://docs.xarray.dev/en/stable/getting-started-guide/installing.html)**
* **numpy**

---

## Workflow

### Part A: DDH Namelist Modification

DDH domains can be added to an existing experiment by modifying the `.toml`
configuration. To create a matrix of point-type DDH domains within a specific
box, use `CreateDDHnamelist.py`. This script reads a previous experiment file
(FA or GRIB) and generates a modification file for Tactus.

#### Configuration
Edit the following variables at the start of `CreateDDHnamelist.py`:

* `INPUT_FILE`: Path to a GRIB/LFA file from an existing experiment.
* `OUTPUT_CONFIG`: Name of the generated modification file (e.g., `ddh_modif.toml`).
* `LONMIN`, `LONMAX`, `LATMIN`, `LATMAX`: Coordinates defining the extraction box.
* `GEOM_FILE`: Name for the output geometry file (contains coordinates and indices).
* `PLOT_DOMAIN`: Set to `True` to output a domain visualization (`dom.png`).

#### Execution
```bash
python CreateDDHnamelist.py
```

---

### Part B: Modifying the Experiment in Tactus

Once the modification file is generated, follow these steps to update your
experiment:

1.  **Edit `ddh_pre.toml`**:
    * **DDH Output Frequency**: Set `general.output_settings.ddh_dl`.
    * **Step Limit Note**: If the array containing write-out steps exceeds 960 (see `arpifs/module/yomct0.F90`), comment out `namelist_update.master.forecast.NAMCT0.NDHFDTS` and set `namelist_update.master.forecast.NAMCT0.NFRDHFD = n` to output every $n^{th}$ step.
    * **Prefix**: Modify `scheduler.ecfvars.case_prefix`, use a suitable name to differentiate the new experiment.

2.  **Generate the Configuration**:
    Copy `ddh_modif.toml` and `ddh_pre.toml` to your Tactus working directory and run:
    ```bash
    poetry run deode case my_original_exp.toml ddh_pre.toml ddh_modif.toml
    ```

---

### Part C: Converting DDH Output to Zarr

Use `DDH2Zarr.py` to combine DDH outputs from all time-steps into a single Zarr file. This script utilizes `Dask` for parallel processing.

#### Configuration
Edit the following variables in `DDH2Zarr.py`:

* `INPUT_DIR`: Directory containing DDH output files.
* `PATTERN`: Glob search pattern (e.g., `DHFDLDEOD+*s`).
* `NWORKER`: Number of Dask workers (ideally equal to available CPU cores).
* `BATCH_SIZE`: Number of files processed per worker; tune based on available RAM.
* `GEOM_FILE`: The geometry file generated in Part A.
* `OUTPUT_FILE`: Path for the resulting Zarr file.

#### Execution
On **ATOS**, it is recommended to use an interactive job for speed:
```bash
ecinteractive -c32 -m 32G -t 0:30:00
python DDH2Zarr.py
```

---

## TODO
- [ ] Implement fluxes using half-level output.
- [ ] Add unit conversion (extensive to intensive).
