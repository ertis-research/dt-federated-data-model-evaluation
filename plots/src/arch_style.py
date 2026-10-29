"""
Colors and short labels per architecture, shared by the ingestion and query figures so that
each architecture keeps the same color across the paper. The query KG baseline ("Centralized")
uses the same color as the ingestion 1-to-1 baseline, since both are the plain centralized KG.
"""

ARCH_COLORS = {
    "Centralized": "#1f77b4",
    "Centralized_1to1": "#1f77b4",
    "Centralized_Batched": "#8c564b",
    "Federated_1ISU": "#ff7f0e",
    "Federated_2ISU": "#2ca02c",
    "Federated_4ISU": "#d62728",
    "Federated_8ISU": "#9467bd",
}

ARCH_LABELS = {
    "Centralized": "Cent.",
    "Centralized_1to1": "Cent. (1-to-1)",
    "Centralized_Batched": "Cent. (Batched)",
    "Federated_1ISU": "Fed. (1 ISU)",
    "Federated_2ISU": "Fed. (2 ISUs)",
    "Federated_4ISU": "Fed. (4 ISUs)",
    "Federated_8ISU": "Fed. (8 ISUs)",
}
