# This recipe can be run with `pangeo-forge-runner` with the CLI command:
# pangeo-forge-runner bake --repo=~/Documents/carbonplan/leap-pgf-example/ -f ~/Documents/carbonplan/leap-pgf-example/feedstock/config.json --Bake.job_name=AGCD --Bake.job_name=agcd


import apache_beam as beam
import zarr
from dataclasses import dataclass
import subprocess
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from pangeo_forge_recipes.transforms import Indexed, T
# from pangeo_forge_big_query import LogToBigQuery, RegisterDatasetToCatalog


# --------------- METADATA AND CATALOGING -------------------------------
# Github url to meta.yml:
meta_yaml_url = (
    "https://github.com/carbonplan/leap-pgf-example/blob/main/feedstock/meta.yaml"
)
dataset_id = 'AGCD'
catalog_table_id = 'LEAP_data_catalog'
table_id = 'carbonplan.leap.test_dataset_catalog'

# -----------------------------------------------------------------------




# Filename Pattern Inputs
target_chunks = {"time": 40}

# Time Range
years = list(range(1971, 1972))  # 2020

# Variable List
variables = ["precip", "tmax"]  # "tmin", "vapourpres_h09", "vapourpres_h15"


def make_filename(variable, time):
    if variable == "precip":
        fpath = f"https://dapds00.nci.org.au/thredds/fileServer/zv2/agcd/v1/{variable}/total/r005/01day/agcd_v1_{variable}_total_r005_daily_{time}.nc"  # noqa: E501
    else:
        fpath = f"https://dapds00.nci.org.au/thredds/fileServer/zv2/agcd/v1/{variable}/mean/r005/01day/agcd_v1_{variable}_mean_r005_daily_{time}.nc"  # noqa: E501
    return fpath


pattern = FilePattern(
    make_filename,
    ConcatDim(name="time", keys=years),
    MergeDim(name="variable", keys=variables),
)

pattern = pattern.prune()

class DropVars(beam.PTransform):
    """
    Custom Beam tranform to drop unused vars
    """

    @staticmethod
    def _drop_vars(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        ds = ds.drop_vars(["crs", "lat_bnds", "lon_bnds", "time_bnds"])
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._drop_vars)



@dataclass
class SkyPlane(beam.PTransform):
    
    dst_store: str

    @staticmethod
    def _init():
        # Add skyplane init
        subprocess.check_call("skyplane init -y --disable-config-azure --disable-config-aws --disable-config-cloudflare", shell=True)

    def _skyplane(self, store):
        self._init()
        # target_store = store.path
        target_store ="gs://carbonplan-scratch/skyplane/target_store/AGDC.zarr"
        # attempt skyplane transfer through subprocess
        transfer_str = f"skyplane cp -r -y {target_store} {self.dst_store}"
        import pdb; pdb.set_trace()

        subprocess.check_call(transfer_str, shell=True)
        
        return self.dst_store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (pcoll
            | "Copy Zarr" >> beam.Map(self._skyplane)
        )
    
AGCD = (
    beam.Create(pattern.items())
    # | OpenURLWithFSSpec()
    # | OpenWithXarray(file_type=pattern.file_type)
    # | DropVars()
    # | StoreToZarr(
    #     store_name="AGCD.zarr",
    #     target_root = 'gs://carbonplan-scratch/skyplane/target_store/',
    #     combine_dims=pattern.combine_dim_keys,
    #     target_chunks=target_chunks,
    #     attrs={"meta_yaml_url": meta_yaml_url},
    # )
    | SkyPlane(dst_store ="gs://carbonplan-scratch/skyplane/dst_store/AGDC.zarr")
    # | RegisterDatasetToCatalog(table_id=table_id, dataset_id=dataset_id, dataset_url='.')
)


with beam.Pipeline() as p:
    p | AGCD