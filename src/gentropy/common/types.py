"""Types and type aliases used in the package."""

from typing import Literal

LD_Population = Literal["afr", "amr", "asj", "eas", "est", "fin", "nfe", "nwe", "seu"]

VariantPopulation = Literal[
    "afr", "amr", "ami", "asj", "eas", "fin", "nfe", "mid", "sas", "remaining"
]
DataSourceType = Literal[
    "gnomad",
    "finngen",
    "gwas_catalog",
    "eqtl_catalog",
    "ukbiobank",
    "open_targets",
    "intervals",
]
