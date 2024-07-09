"""Interface for application configuration."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List

from hail import __file__ as hail_location
from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class SessionConfig:
    """Session configuration."""

    start_hail: bool = False
    write_mode: str = "errorifexists"
    spark_uri: str = "local[*]"
    hail_home: str = os.path.dirname(hail_location)
    extended_spark_conf: dict[str, str] | None = field(default_factory=dict[str, str])
    _target_: str = "gentropy.common.session.Session"


@dataclass
class StepConfig:
    """Base step configuration."""

    session: SessionConfig
    defaults: List[Any] = field(
        default_factory=lambda: [{"session": "base_session"}, "_self_"]
    )


@dataclass
class ColocalisationConfig(StepConfig):
    """Colocalisation step configuration."""

    credible_set_path: str = MISSING
    study_index_path: str = MISSING
    coloc_path: str = MISSING
    colocalisation_method: str = MISSING
    _target_: str = "gentropy.colocalisation.ColocalisationStep"


@dataclass
class GeneIndexConfig(StepConfig):
    """Gene index step configuration."""

    target_path: str = MISSING
    gene_index_path: str = MISSING
    _target_: str = "gentropy.gene_index.GeneIndexStep"


@dataclass
class GWASCatalogStudyCurationConfig(StepConfig):
    """GWAS Catalog study curation step configuration."""

    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_sumstats_lut: str = MISSING
    gwas_catalog_study_curation_out: str = MISSING
    gwas_catalog_study_curation_file: str = MISSING
    _target_: str = "gentropy.gwas_catalog_study_curation.GWASCatalogStudyCurationStep"


@dataclass
class GWASCatalogStudyInclusionConfig(StepConfig):
    """GWAS Catalog study inclusion step configuration."""

    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_associations_file: str = MISSING
    gwas_catalog_study_curation_file: str = MISSING
    variant_annotation_path: str = MISSING
    harmonised_study_file: str = MISSING
    criteria: str = MISSING
    inclusion_list_path: str = MISSING
    exclusion_list_path: str = MISSING
    _target_: str = (
        "gentropy.gwas_catalog_study_inclusion.GWASCatalogStudyInclusionGenerator"
    )


@dataclass
class GWASCatalogIngestionConfig(StepConfig):
    """GWAS Catalog ingestion step configuration."""

    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    variant_annotation_path: str = MISSING
    catalog_studies_out: str = MISSING
    catalog_associations_out: str = MISSING
    gwas_catalog_study_curation_file: str | None = None
    inclusion_list_path: str | None = None
    _target_: str = "gentropy.gwas_catalog_ingestion.GWASCatalogIngestionStep"


@dataclass
class GWASCatalogSumstatsPreprocessConfig(StepConfig):
    """GWAS Catalog sumstat preprocess step configuration."""

    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING
    _target_: str = (
        "gentropy.gwas_catalog_sumstat_preprocess.GWASCatalogSumstatsPreprocessStep"
    )


@dataclass
class EqtlCatalogueConfig(StepConfig):
    """eQTL Catalogue step configuration."""

    eqtl_catalogue_paths_imported: str = MISSING
    eqtl_catalogue_study_index_out: str = MISSING
    eqtl_catalogue_credible_sets_out: str = MISSING
    mqtl_quantification_methods_blacklist: list[str] = field(default_factory=lambda: [])
    _target_: str = "gentropy.eqtl_catalogue.EqtlCatalogueStep"


@dataclass
class FinngenStudiesConfig(StepConfig):
    """FinnGen study index step configuration."""

    finngen_study_index_out: str = MISSING
    _target_: str = "gentropy.finngen_studies.FinnGenStudiesStep"


@dataclass
class FinngenSumstatPreprocessConfig(StepConfig):
    """FinnGen study index step configuration."""

    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING
    _target_: str = "gentropy.finngen_sumstat_preprocess.FinnGenSumstatPreprocessStep"


@dataclass
class FinngenFinemappingConfig(StepConfig):
    """FinnGen fine mapping ingestion step configuration."""

    finngen_finemapping_results_path: str = MISSING
    finngen_finemapping_summaries_path: str = MISSING
    finngen_release_prefix: str = MISSING
    finngen_finemapping_out: str = MISSING
    _target_: str = (
        "gentropy.finngen_finemapping_ingestion.FinnGenFinemappingIngestionStep"
    )


@dataclass
class LDIndexConfig(StepConfig):
    """LD index step configuration."""

    session: Any = field(
        default_factory=lambda: {
            "start_hail": True,
        }
    )
    ld_index_out: str = MISSING
    min_r2: float = 0.5
    ld_matrix_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm"
    ld_index_raw_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht"
    liftover_ht_path: str = "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht"
    grch37_to_grch38_chain_path: str = (
        "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
    )
    ld_populations: list[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
        ]
    )
    use_version_from_input: bool = False
    _target_: str = "gentropy.gnomad_ingestion.LDIndexStep"


@dataclass
class LDBasedClumpingConfig(StepConfig):
    """LD based clumping step configuration."""

    study_locus_input_path: str = MISSING
    study_index_path: str = MISSING
    ld_index_path: str = MISSING
    clumped_study_locus_output_path: str = MISSING
    _target_: str = "gentropy.ld_based_clumping.LDBasedClumpingStep"


@dataclass
class LocusToGeneConfig(StepConfig):
    """Locus to gene step configuration."""

    session: Any = field(
        default_factory=lambda: {
            "extended_spark_conf": {
                "spark.dynamicAllocation.enabled": "false",
                "spark.driver.memory": "48g",
                "spark.executor.memory": "48g",
                "spark.sql.shuffle.partitions": "800",
            }
        }
    )
    run_mode: str = MISSING
    predictions_path: str = MISSING
    credible_set_path: str = MISSING
    variant_gene_path: str = MISSING
    colocalisation_path: str = MISSING
    study_index_path: str = MISSING
    model_path: str | None = None
    feature_matrix_path: str | None = None
    gold_standard_curation_path: str | None = None
    gene_interactions_path: str | None = None
    features_list: list[str] = field(
        default_factory=lambda: [
            # average distance of all tagging variants to gene TSS
            "distanceTssMean",
            # minimum distance of all tagging variants to gene TSS
            "distanceTssMinimum",
            # maximum vep consequence score of the locus 95% credible set among all genes in the vicinity
            "vepMaximumNeighborhood",
            # maximum vep consequence score of the locus 95% credible set split by gene
            "vepMaximum",
            # mean vep consequence score of the locus 95% credible set among all genes in the vicinity
            "vepMeanNeighborhood",
            # mean vep consequence score of the locus 95% credible set split by gene
            "vepMean",
            # max clpp for each (study, locus, gene) aggregating over all eQTLs
            "eqtlColocClppMaximum",
            # max clpp for each (study, locus) aggregating over all eQTLs
            "eqtlColocClppMaximumNeighborhood",
            # max clpp for each (study, locus, gene) aggregating over all pQTLs
            "pqtlColocClppMaximum",
            # max clpp for each (study, locus) aggregating over all pQTLs
            "pqtlColocClppMaximumNeighborhood",
            # max clpp for each (study, locus, gene) aggregating over all sQTLs
            "sqtlColocClppMaximum",
            # max clpp for each (study, locus) aggregating over all sQTLs
            "sqtlColocClppMaximumNeighborhood",
            # max clpp for each (study, locus) aggregating over all tuQTLs
            "tuqtlColocClppMaximum",
            # max clpp for each (study, locus, gene) aggregating over all tuQTLs
            "tuqtlColocClppMaximumNeighborhood",
            # max log-likelihood ratio value for each (study, locus, gene) aggregating over all eQTLs
            "eqtlColocLlrMaximum",
            # max log-likelihood ratio value for each (study, locus) aggregating over all eQTLs
            "eqtlColocLlrMaximumNeighborhood",
            # max log-likelihood ratio value for each (study, locus, gene) aggregating over all pQTLs
            "pqtlColocLlrMaximum",
            # max log-likelihood ratio value for each (study, locus) aggregating over all pQTLs
            "pqtlColocLlrMaximumNeighborhood",
            # max log-likelihood ratio value for each (study, locus, gene) aggregating over all sQTLs
            "sqtlColocLlrMaximum",
            # max log-likelihood ratio value for each (study, locus) aggregating over all sQTLs
            "sqtlColocLlrMaximumNeighborhood",
            # max log-likelihood ratio value for each (study, locus, gene) aggregating over all tuQTLs
            "tuqtlColocLlrMaximum",
            # max log-likelihood ratio value for each (study, locus) aggregating over all tuQTLs
            "tuqtlColocLlrMaximumNeighborhood",
        ]
    )
    hyperparameters: dict[str, Any] = field(
        default_factory=lambda: {
            "n_estimators": 100,
            "max_depth": 5,
            "loss": "log_loss",
        }
    )
    wandb_run_name: str | None = None
    hf_hub_repo_id: str | None = "opentargets/locus_to_gene"
    download_from_hub: bool = True
    _target_: str = "gentropy.l2g.LocusToGeneStep"


@dataclass
class PICSConfig(StepConfig):
    """PICS step configuration."""

    study_locus_ld_annotated_in: str = MISSING
    picsed_study_locus_out: str = MISSING
    _target_: str = "gentropy.pics.PICSStep"


@dataclass
class UkbPppEurConfig(StepConfig):
    """UKB PPP (EUR) ingestion step configuration."""

    raw_study_index_path: str = MISSING
    raw_summary_stats_path: str = MISSING
    tmp_variant_annotation_path: str = MISSING
    variant_annotation_path: str = MISSING
    study_index_output_path: str = MISSING
    summary_stats_output_path: str = MISSING
    _target_: str = "gentropy.ukb_ppp_eur_sumstat_preprocess.UkbPppEurStep"


@dataclass
class GnomadVariantConfig(StepConfig):
    """Gnomad variant ingestion step configuration."""

    session: Any = field(
        default_factory=lambda: {
            "start_hail": True,
        }
    )
    variant_annotation_path: str = MISSING
    gnomad_genomes_path: str = "gs://gcp-public-data--gnomad/release/4.0/ht/genomes/gnomad.genomes.v4.0.sites.ht/"
    gnomad_variant_populations: list[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "ami",  # Amish ancestry
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "mid",  # Middle Eastern
            "sas",  # South Asian
            "remaining",  # Other
        ]
    )
    use_version_from_input: bool = False
    _target_: str = "gentropy.gnomad_ingestion.GnomadVariantIndexStep"


@dataclass
class VariantIndexConfig(StepConfig):
    """Variant index step configuration."""

    vep_output_json_path: str = MISSING
    variant_index_path: str = MISSING
    gnomad_variant_annotations_path: str | None = None
    _target_: str = "gentropy.variant_index.VariantIndexStep"


@dataclass
class VariantToGeneConfig(StepConfig):
    """V2G step configuration."""

    variant_index_path: str = MISSING
    gene_index_path: str = MISSING
    vep_consequences_path: str = MISSING
    liftover_chain_file_path: str = MISSING
    liftover_max_length_difference: int = 100
    max_distance: int = 500_000
    approved_biotypes: List[str] = field(
        default_factory=lambda: [
            "protein_coding",
            "3prime_overlapping_ncRNA",
            "antisense",
            "bidirectional_promoter_lncRNA",
            "IG_C_gene",
            "IG_D_gene",
            "IG_J_gene",
            "IG_V_gene",
            "lincRNA",
            "macro_lncRNA",
            "non_coding",
            "sense_intronic",
            "sense_overlapping",
        ]
    )
    interval_sources: Dict[str, str] = field(default_factory=dict)
    v2g_path: str = MISSING
    _target_: str = "gentropy.variant_to_gene.V2GStep"


@dataclass
class LocusBreakerClumpingConfig(StepConfig):
    """Locus breaker clumping step configuration."""

    session: Any = field(
        default_factory=lambda: {
            "start_hail": True,
        }
    )
    distance_cutoff: int = 250_000
    flanking_distance: int = 100_000
    baseline_pvalue_cutoff: float = 1e-5


@dataclass
class WindowBasedClumpingStepConfig(StepConfig):
    """Window-based clumping step configuration."""

    session: Any = field(
        default_factory=lambda: {
            "start_hail": True,
        }
    )
    summary_statistics_input_path: str = MISSING
    study_locus_output_path: str = MISSING
    gwas_significance: float = 1e-8
    distance: int = 500_000
    collect_locus: bool = False
    collect_locus_distance: int = 500_000
    inclusion_list_path: str | None = None
    _target_: str = "gentropy.window_based_clumping.WindowBasedClumpingStep"


@dataclass
class FinemapperConfig(StepConfig):
    """SuSiE fine-mapper step configuration."""

    session: Any = field(
        default_factory=lambda: {
            "start_hail": True,
        }
    )
    study_locus_to_finemap: str = MISSING
    study_locus_collected_path: str = MISSING
    study_index_path: str = MISSING
    output_path: str = MISSING
    max_causal_snps: int = MISSING
    primary_signal_pval_threshold: float = MISSING
    secondary_signal_pval_threshold: float = MISSING
    purity_mean_r2_threshold: float = MISSING
    purity_min_r2_threshold: float = MISSING
    cs_lbf_thr: float = MISSING
    sum_pips: float = MISSING
    susie_est_tausq: bool = MISSING
    run_carma: bool = MISSING
    run_sumstat_imputation: bool = MISSING
    carma_time_limit: int = MISSING
    imputed_r2_threshold: float = MISSING
    ld_score_threshold: float = MISSING
    output_path_log: str = MISSING
    _target_: str = "gentropy.susie_finemapper.SusieFineMapperStep"


@dataclass
class GWASQCStep(StepConfig):
    """GWAS QC step configuration."""

    gwas_path: str = MISSING
    output_path: str = MISSING
    studyid: str = MISSING
    _target_: str = "gentropy.sumstat_qc_step.SummaryStatisticsQCStep"


@dataclass
class Config:
    """Application configuration."""

    # this is unfortunately verbose due to @dataclass limitations
    defaults: List[Any] = field(default_factory=lambda: ["_self_", {"step": MISSING}])
    step: StepConfig = MISSING
    datasets: dict[str, str] = field(default_factory=dict)


def register_config() -> None:
    """Register configuration."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(group="step/session", name="base_session", node=SessionConfig)
    cs.store(group="step", name="colocalisation", node=ColocalisationConfig)
    cs.store(group="step", name="eqtl_catalogue", node=EqtlCatalogueConfig)
    cs.store(group="step", name="gene_index", node=GeneIndexConfig)
    cs.store(
        group="step",
        name="gwas_catalog_study_curation",
        node=GWASCatalogStudyCurationConfig,
    )
    cs.store(
        group="step",
        name="gwas_catalog_study_inclusion",
        node=GWASCatalogStudyInclusionConfig,
    )
    cs.store(
        group="step", name="gwas_catalog_ingestion", node=GWASCatalogIngestionConfig
    )
    cs.store(
        group="step",
        name="gwas_catalog_sumstat_preprocess",
        node=GWASCatalogSumstatsPreprocessConfig,
    )
    cs.store(group="step", name="ld_based_clumping", node=LDBasedClumpingConfig)
    cs.store(group="step", name="ld_index", node=LDIndexConfig)
    cs.store(group="step", name="locus_to_gene", node=LocusToGeneConfig)
    cs.store(group="step", name="finngen_studies", node=FinngenStudiesConfig)
    cs.store(
        group="step",
        name="finngen_sumstat_preprocess",
        node=FinngenSumstatPreprocessConfig,
    )

    cs.store(
        group="step",
        name="finngen_finemapping_ingestion",
        node=FinngenFinemappingConfig,
    )

    cs.store(group="step", name="pics", node=PICSConfig)
    cs.store(group="step", name="variant_annotation", node=GnomadVariantConfig)
    cs.store(group="step", name="ukb_ppp_eur_sumstat_preprocess", node=UkbPppEurConfig)
    cs.store(group="step", name="variant_index", node=VariantIndexConfig)
    cs.store(group="step", name="variant_to_gene", node=VariantToGeneConfig)
    cs.store(
        group="step", name="window_based_clumping", node=WindowBasedClumpingStepConfig
    )
    cs.store(group="step", name="susie_finemapping", node=FinemapperConfig)
