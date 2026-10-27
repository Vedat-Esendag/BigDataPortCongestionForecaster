# Notebooks

Exploration only. Nothing in the pipeline imports from here.

Rules of thumb:

- Reusable logic goes into `spark/common/` (cleaning, port events, features) and is imported, not copied.
  A notebook that re-implements a feature will quietly disagree with the live job.
- Clear outputs before committing, and never paste keys or personal data into a notebook.
- Small samples can live next to the notebook. Real data (DMA files) goes in `data/`, which git ignores.
