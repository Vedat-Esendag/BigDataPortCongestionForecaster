"""F8: find and load the latest trained ETA model from HDFS.

Models live at <model root>/v=<UTC timestamp>/ (see common.model_paths). The live job calls
`load_latest_model` at start-up and can call it again on a timer to pick up a new version.

STUB: the folder listing is implemented but untested against a real HDFS, and loading is minimal.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from common.config import model_root
from common.model_paths import latest_version


def list_version_dirs(spark: SparkSession, root: str) -> list[str]:
    """Names of the sub-folders of `root` (empty when `root` does not exist yet)."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    return [status.getPath().getName() for status in fs.listStatus(path) if status.isDirectory()]


def load_latest_model(spark: SparkSession, root: str | None = None):
    """Load the newest model under `root`.

    Returns:
        (model, version_name). Both are None when no model has been trained yet, in which case
        the live job keeps using the placeholder predictor.

    TODO(F8): also load the residual window (lower and upper quantiles) saved with the model.
    TODO(F8): add a periodic reload so a freshly trained model is picked up without a restart.
    """
    root = root or model_root()
    version = latest_version(list_version_dirs(spark, root))
    if version is None:
        return None, None

    from pyspark.ml import PipelineModel

    return PipelineModel.load(f"{root}/{version}/model"), version
