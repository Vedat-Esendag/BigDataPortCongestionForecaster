"""Code shared by the streaming job and the batch and training jobs.

Cleaning, port event detection and feature building live here and only here, so the features a
model is trained on come from the same code as the features it sees live. Ship this package with
`spark-submit --py-files common.zip` (see `make spark-package`).
"""
