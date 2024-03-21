

# pangeo-forge-runner bake \
#     --config feedstock/config.py \
#     --repo . \
#     --Bake.job_name=test1 \
#     --prune





import apache_beam as beam
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

from leap_data_management_utils import RegisterDatasetToCatalog


# --------------- METADATA AND CATALOGING -------------------------------
# Github url to meta.yml:
meta_yaml_url = (
    "https://github.com/carbonplan/leap-pgf-example/blob/main/feedstock/meta3.yaml"
)

dataset_id = "dataset_2"
# table_id = "carbonplan.leap.test_dataset_catalog"
# -----------------------------------------------------------------------





dates = pd.date_range("1981-09-01", "1981-09-03", freq="D")

URL_FORMAT = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
    "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


recipe = (
    beam.Create(pattern.items()) 
    | OpenURLWithFSSpec() 
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name="test_dataset3.zarr",
        combine_dims=pattern.combine_dim_keys,
        attrs={"meta_yaml_url": meta_yaml_url},
    ))





    # | "Log to carbonplan BQ Catalog Table"
    # >> RegisterDatasetToCatalog(table_id=table_id, dataset_id=dataset_id)



# import pandas as pd 

# dataset_ids = ['dataset_1','dataset_2','dataset_3']
# time_stamps = ['2024-03-21 15:10:00', '2024-03-21 15:15:00', '2024-03-21 15:20:00']
# dataset_urls = ['s3://carbonplan-scratch/leap_ex/test_dataset1.zarr/','s3://carbonplan-scratch/leap_ex/test_dataset3.zarr/','s3://carbonplan-scratch/leap_ex/test_dataset3.zarr/']

# df = pd.DataFrame({'dataset_id':dataset_ids, 'timestamp': time_stamps, 'dataset_url':dataset_urls})