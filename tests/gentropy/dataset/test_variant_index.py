"""Tests on variant index generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.v2g import V2G
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    pass


def test_variant_index_creation(mock_variant_index: VariantIndex) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_variant_index, VariantIndex)


def test_get_plof_v2g(
    mock_variant_index: VariantIndex, mock_gene_index: GeneIndex
) -> None:
    """Test get_plof_v2g with mock variant annotation."""
    assert isinstance(mock_variant_index.get_plof_v2g(mock_gene_index), V2G)


def test_get_distance_to_tss(
    mock_variant_index: VariantIndex, mock_gene_index: GeneIndex
) -> None:
    """Test get_distance_to_tss with mock variant annotation."""
    assert isinstance(mock_variant_index.get_distance_to_tss(mock_gene_index), V2G)
