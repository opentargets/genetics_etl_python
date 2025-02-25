"""Provides namespace for features to use them in containers."""

from enum import StrEnum


class L2GDistanceFeatureName(StrEnum):
    """Class representing L2G available feature names."""

    DISTANCE_TSS_MEAN = "distanceTssMean"
    DISTANCE_TSS_MEAN_NEIGHBOURHOOD = "distanceTssMeanNeighbourhood"
    DISTANCE_SENTINEL_TSS = "distanceSentinelTss"
    DISTANCE_SENTINEL_TSS_NEIGHBOURHOOD = "distanceSentinelTssNeighbourhood"
    DISTANCE_FOOTPRINT_MEAN = "distanceFootprintMean"
    DISTANCE_FOOTPRINT_MEAN_NEIGHBOURHOOD = "distanceFootprintMeanNeighbourhood"
    DISTANCE_SENTINEL_FOOTPRINT = "distanceSentinelFootprint"
    DISTANCE_SENTINEL_FOOTPRINT_NEIGHBOURHOOD = "distanceSentinelFootprintNeighbourhood"


class L2GColocFeatureName(StrEnum):
    """Class representing L2G Colocalisation feature names."""

    # ecaviar
    EQTL_COLOC_CLPP_MAXIMUM = "eQtlColocClppMaximum"
    EQTL_COLOC_CLPP_MAXIMUM_NEIGHBOURHOOD = "eQtlColocClppMaximumNeighbourhood"
    PQTL_COLOC_CLPP_MAXIMUM = "pQtlColocClppMaximum"
    PQTL_COLOC_CLPP_MAXIMUM_NEIGHBOURHOOD = "pQtlColocClppMaximumNeighbourhood"
    SQTL_COLOC_CLPP_MAXIMUM = "sQtlColocClppMaximum"
    SQTL_COLOC_CLPP_MAXIMUM_NEIGHBOURHOOD = "sQtlColocClppMaximumNeighbourhood"

    # coloc
    EQTL_COLOC_H4_MAXIMUM = "eQtlColocH4Maximum"
    EQTL_COLOC_H4_MAXIMUM_NEIGHBOURHOOD = "eQtlColocH4MaximumNeighbourhood"
    PQTL_COLOC_H4_MAXIMUM = "pQtlColocH4Maximum"
    PQTL_COLOC_H4_MAXIMUM_NEIGHBOURHOOD = "pQtlColocH4MaximumNeighbourhood"
    SQTL_COLOC_H4_MAXIMUM = "sQtlColocH4Maximum"
    SQTL_COLOC_H4_MAXIMUM_NEIGHBOURHOOD = "sQtlColocH4MaximumNeighbourhood"


class L2GFeatureName(L2GColocFeatureName, L2GDistanceFeatureName):
    """Class representing all L2G features."""

    GENE_COUNT_500_KB = "geneCount500kb"
    PROTEIN_CODING_GENE_COUNT_500_KB = "proteinGeneCount500kb"
    PROTEIN_CODING = "isProteinCoding"
    CREDIBLE_SET_CONFIDENCE = "credibleSetConfidence"
