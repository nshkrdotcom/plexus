"""Render retained experiment metrics; pip install matplotlib in a virtualenv."""
import csv
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

root = Path("artifacts/calibration")
data = json.loads((root / "report.json").read_text())
fig, axes = plt.subplots(1, 4, figsize=(14, 3.5), sharex=True, sharey=True)
rows = []
for axis, (phrasing, result) in zip(axes, data["reports"].items()):
    axis.plot([0, 1], [0, 1], "k--", linewidth=0.8, label="ideal")
    for kind in ("raw", "calibrated"):
        bins = result[kind]["reliability"]
        axis.plot([b["mean_predicted"] for b in bins], [b["observed_rate"] for b in bins], "o-", label=kind)
        rows.extend({"phrasing": phrasing, "prediction": kind, **b} for b in bins)
    axis.set(title=phrasing, xlabel="Predicted probability", xlim=(-0.03, 1.03), ylim=(-0.03, 1.03))
axes[0].set_ylabel("Observed prime fraction")
axes[-1].legend()
fig.suptitle("Held-out primality reliability — 30 integers, three correlated phrasings")
fig.tight_layout()
fig.savefig(root / "reliability.png", dpi=180)
fig.savefig(root / "reliability.svg")
with (root / "reliability.csv").open("w") as stream:
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)

svg = root / "reliability.svg"
svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
