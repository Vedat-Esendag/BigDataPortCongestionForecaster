"""F7: train the ETA delay model with MLlib.

Target: delay = actual arrival minus broadcast ETA, in minutes. The live job adds the predicted
delay to the broadcast ETA. The 70% window comes from the validation residuals: the 15th and 85th
percentile of (actual delay minus predicted delay), stored next to the model.

Output: <model root>/v=<UTC timestamp>/  (default root hdfs:///models/eta, see common.model_paths)

STUB: the structure is here, every step raises NotImplementedError.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession

from common.config import load_thresholds, model_root
from common.features import FEATURE_COLUMNS
from common.model_paths import version_dir_name

log = logging.getLogger("train_eta")


def load_training_data(spark: SparkSession, start: str, end: str) -> DataFrame:
    """Return feature rows with a `delay_minutes` label for arrivals between start and end.

    TODO(F7): read cleaned history from the lake, build features with common.features.build_features
              (the SAME function the live job uses), and compute the label from port events.
    """
    raise NotImplementedError("TODO(F7): load_training_data")


def fit_model(train: DataFrame):
    """Fit a VectorAssembler + GBTRegressor pipeline on FEATURE_COLUMNS.

    TODO(F7): GBTRegressor(labelCol="delay_minutes"), tune maxDepth and maxIter on validation,
              handle missing weather with an imputer.
    """
    raise NotImplementedError(f"TODO(F7): fit_model on {len(FEATURE_COLUMNS)} features")


def residual_window(model, validation: DataFrame, coverage: float) -> tuple[float, float]:
    """Lower and upper residual quantiles that contain `coverage` of the validation errors.

    TODO(F7): residual = delay_minutes - prediction; return the (1-c)/2 and 1-(1-c)/2 quantiles.
    """
    raise NotImplementedError("TODO(F7): residual_window")


def save_model(model, window: tuple[float, float], root: str, moment: datetime) -> str:
    """Write the model and its residual window to <root>/v=<timestamp>/ and return that path.

    TODO(F7): model.write().save(f"{path}/model") and a small JSON file with the window.
    """
    path = f"{root.rstrip('/')}/{version_dir_name(moment)}"
    raise NotImplementedError(f"TODO(F7): save_model to {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the ETA delay model")
    parser.add_argument("--start", required=True, help="first training day, yyyy-MM-dd")
    parser.add_argument("--end", required=True, help="last training day, yyyy-MM-dd")
    parser.add_argument("--model-root", default=model_root())
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("train-eta").getOrCreate()
    coverage = load_thresholds().window_coverage

    data = load_training_data(spark, args.start, args.end)
    # TODO(F7): split by time, not at random, so validation looks like the future.
    train, validation = data.randomSplit([0.8, 0.2], seed=42)
    model = fit_model(train)
    window = residual_window(model, validation, coverage)
    path = save_model(model, window, args.model_root, datetime.now(UTC))
    log.info("saved model to %s", path)


if __name__ == "__main__":
    main()
