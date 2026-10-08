"""Experimental probability calibration, wallet flow and paper evaluation.

Historical recalibration results depend on the price baseline, timestamp and
fee assumptions. Improvements against stale last trades did not establish an
executable advantage against contemporaneous quotes. See docs/findings.md.

Dataset construction, isotonic calibration, scoring and outcome grading remain
available as research components. Stored prices use decimal strings. No module
in this package submits orders or certifies a profitable strategy.
"""

from .calibrate import train
from .grade import grade
from .ofi import ofi_score
from .score import model_score

__all__ = ["train", "grade", "model_score", "ofi_score"]
