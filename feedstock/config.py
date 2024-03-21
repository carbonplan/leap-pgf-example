# vi local-runner-config.py
c.Bake.bakery_class = 'pangeo_forge_runner.bakery.local.LocalDirectBakery'
c.Bake.prune = True
c.LocalDirectBakery.num_workers = 4

c.MetadataCacheStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# Metadata cache should be per `{{job_name}}`, as kwargs changing can change metadata
c.MetadataCacheStorage.root_path = 'local_storage/metadatacache/'

# c.TargetStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# c.TargetStorage.root_path = 'local_storage/target/'

c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
c.TargetStorage.root_path = f"s3://carbonplan-scratch/leap_ex"

# c.InputCacheStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# c.InputCacheStorage.root_path = 'local_storage/cache/'


