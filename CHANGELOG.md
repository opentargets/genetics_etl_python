



# CHANGELOG





## v1.4.0 (2024-06-14)
### ✨ Feature


- \[**config**\] 24.06 data release fixes (#639) [\@Szymon Szyszkowski](mailto:69353402+project-defiant@users.noreply.github.com)


-  credible set quality filtering (#640) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  enable interface with gcp secrets manager (#635) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  adding locus-breaker clumping method (#634) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**spark-helpers**\] enforce schema of returned objects (#617) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**qtl**\] ingest credible sets from single cell derived QTLs (#630) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**data_release**\] preparation for 24.06 data release (#633) [\@Szymon Szyszkowski](mailto:69353402+project-defiant@users.noreply.github.com)


- \[**config**\] extract gwas_significance parameter to step configuration (#628) [\@Szymon Szyszkowski](mailto:69353402+project-defiant@users.noreply.github.com)


- \[**config**\] gnomAD steps configuration extraction and versioning (#620) [\@Szymon Szyszkowski](mailto:69353402+project-defiant@users.noreply.github.com)


-  fine-mapping simulations class (#618) [\@Yakov](mailto:yt4@sanger.ac.uk)

### 🐛 Fix


- \[**L2GPrediction**\] schema validation (#642) [\@Szymon Szyszkowski](mailto:69353402+project-defiant@users.noreply.github.com)


- \[**docs**\] fixed typo in l2g_prediciton schema page (#629) [\@Szymon Szyszkowski](mailto:69353402+project-defiant@users.noreply.github.com)


- \[**SummaryStatistics**\] fix in sanity_filter (#623) [\@Yakov](mailto:yt4@sanger.ac.uk)


- \[**susie_finemapper**\] fix in the fine-mapper in case of sum stat imputation is False (#627) [\@Yakov](mailto:yt4@sanger.ac.uk)


- \[**susie_finemapper**\] correct syntax for saving the logs df (#625) [\@Kirill Tsukanov](mailto:tskir@users.noreply.github.com)


- \[**ld**\] correct syntax for the static method (#624) [\@Kirill Tsukanov](mailto:tskir@users.noreply.github.com)


- \[**docs**\] update roadmap.md (#622) [\@buniello](mailto:30833755+buniello@users.noreply.github.com)


- \[**spark helpers**\] sorter function updated to avoid crash (#613) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### 🏗 Build


- \[**deps-dev**\] bump dbldatagen from 0.3.5 to 0.4.0 (#637) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump scikit-learn from 1.4.0 to 1.5.0 (#638) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.24.0 to 8.25.0 (#636) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump wandb from 0.16.2 to 0.17.0 (#606) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pep8-naming from 0.13.3 to 0.14.1 (#616) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump typing-extensions from 4.11.0 to 4.12.1 (#632) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] use pandas[gcp, parquet] (#626) [\@Kirill Tsukanov](mailto:tskir@users.noreply.github.com)

## v1.3.0 (2024-05-17)
### ✨ Feature


- \[**airflow**\] include COLOC as a node in the DAG (#530) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  check for if no overlapping variants in LD index (#594) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  add purity qc metrics to fine-mapping (#592) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  lighter dockerfile (#585) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  github action to upload docker image to registry (#588) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  functionality added to StudyLocus.find_overlaps() for finding within-study overlaps (#587) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  susie_finemapper_one_studylocus_row_v3_dev_ss_gathered (#586) [\@Yakov](mailto:yt4@sanger.ac.uk)


- \[**sumstat qc**\] adding methods for QC of summary statistics (#455) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  changing locus window to locus radius to be consistent with other functions (#582) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  updating step config file (#580) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  dockerise gentropy python package (#579) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  adding init to finemapping step (#577) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  adding notebook for mapping EFOs for the FinnGen study index (#575) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  add benchmarking for fine-mapping using Alzheimer as example (#572) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  add FM step with carma and sumstat imputation (#568) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  susie_finemapper_ss_gathered() (#567) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  the FineMapper function for one locus (#564) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  LD index and block matrix extraction for a studyLocus (#463) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  add the step class for fine-mapping (#554) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  notebook to run qc metrics each release (#541) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)


- \[**l2g**\] distance features based on weighted score (#545) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**sumstat imputation**\] adding class for sumstat imputation (#490) [\@Yakov](mailto:yt4@sanger.ac.uk)


- \[**coloc**\] single SNP case (#511) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)


-  extract credible sets and studies from all eQTL Catalogue finemapping results (#518) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

### 🐛 Fix


-  fix python-semantic-release version (again) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix python-semantic-release version [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  docker action fixes v3 (#591) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  docker action fixes (#590) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  docker action fixes (#589) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  updating config.py argument for finemapper (#584) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  minor updates and bug fixes (#543) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  update error in config.py (#583) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  removing all duplicated variants in sumstats for finemapping functions (#574) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


-  adding deduplication for GWAS in locus (#573) [\@Yakov](mailto:yt4@sanger.ac.uk)


- \[**sumstats**\] correct study id for dir of finngen studies (#551) [\@Joris Louwen](mailto:62542407+louwenjjr@users.noreply.github.com)


- \[**coloc**\] handle cases when the bayes factors are null (#556) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  small fixes in susie defaults (#552) [\@Yakov](mailto:yt4@sanger.ac.uk)


- \[**dag**\] sorting out the checks for the existence of GCP folders [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  read function for thurman data to include first line (#534) [\@Joris Louwen](mailto:62542407+louwenjjr@users.noreply.github.com)


-  pin version of commitlint (#533) [\@Joris Louwen](mailto:62542407+louwenjjr@users.noreply.github.com)


-  updating gwas catalog test files following schema changes [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  fixing lock file [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  removing fsspec from dependencies [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  sorting out merge conflicts [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  fixing merge conflict [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  pr labeller patterns (#523) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  fixing merge conflict [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  reverting some changes [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### ♻️ Refactor


-  moving all variant coordinates to GnomAD (#566) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  tidying up the ETL DAG [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### ⚡️ Performance


- \[**l2g**\] optimise extraction of features from colocalisation results (#553) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**clump**\] refactored window based clumping (#492) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**l2g**\] streamline feature generation (#544) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

### ✅ Test


- \[**method**\] improved performance in coloc tests (#536) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)

### 🏗 Build


- \[**deps-dev**\] bump mypy from 1.9.0 to 1.10.0 (#604) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.22.1 to 8.24.0 (#603) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump typing-extensions from 4.10.0 to 4.11.0 (#602) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.3.2 to 0.4.3 (#596) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest-xdist from 3.5.0 to 3.6.1 (#597) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest from 8.1.0 to 8.2.0 (#598) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump interrogate from 1.5.0 to 1.7.0 (#599) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pymdown-extensions from 10.7 to 10.8.1 (#600) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump python-semantic-release from 9.1.0 to 9.4.1 (#571) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest-cov from 4.1.0 to 5.0.0 (#560) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pre-commit from 3.6.0 to 3.7.0 (#559) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump lxml from 5.1.0 to 5.2.1 (#569) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump deptry from 0.14.0 to 0.16.1 (#570) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocstrings-python from 1.8.0 to 1.9.0 (#549) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump deptry from 0.12.0 to 0.14.0 (#548) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mypy from 1.8.0 to 1.9.0 (#532) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.2.0 to 0.3.2 (#531) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

### 👷‍♂️ Ci


-  bugfix in PR labeller (#537) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🚀 Chore


-  pre-commit autoupdate (#601) [\@pre-commit-ci[bot]](mailto:66853113+pre-commit-ci[bot]@users.noreply.github.com)


-  pre-commit autoupdate (#593) [\@pre-commit-ci[bot]](mailto:66853113+pre-commit-ci[bot]@users.noreply.github.com)


-  pre-commit autoupdate (#561) [\@pre-commit-ci[bot]](mailto:66853113+pre-commit-ci[bot]@users.noreply.github.com)


-  pre-commit autoupdate (#550) [\@pre-commit-ci[bot]](mailto:66853113+pre-commit-ci[bot]@users.noreply.github.com)


- \[**l2g**\] log annotated gold standards in w&amp;b (#546) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  pre-commit autoupdate (#539) [\@pre-commit-ci[bot]](mailto:66853113+pre-commit-ci[bot]@users.noreply.github.com)


-  apply suggestions from code review [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  update ruff pre-commit and rules (#522) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  add sqtl, tuqtl, pqtl features to l2g inclusion list [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add tuqtl features to L2Gfeaturematrix schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  bumping for 24.03 release [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  resolve merge commit [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

## v1.2.0 (2024-03-07)
### ✨ Feature


- \[**coloc**\] single SNP case (#511) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)


-  extract credible sets and studies from all eQTL Catalogue finemapping results (#518) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**CARMA**\] adding time limited CARMA (#475) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  extract credible sets and studies from eQTL Catalogue finemapping results (#514) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

### 🐛 Fix


-  pr labeller patterns (#523) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  pull-request labeler fixes (#520) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  updating study index generation from gwas flat files [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  updating cohort parser to new GWAS Catalog format [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### 🏗 Build


- \[**deps-dev**\] bump pytest from 8.0.1 to 8.1.0 (#516) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-git-committers-plugin-2 from 2.2.3 to 2.3.0 (#505) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump typing-extensions from 4.9.0 to 4.10.0 (#506) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.21.0 to 8.22.1 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

### 👷‍♂️ Ci


-  precommit checks for poetry TOML and lock (#526) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  pull request template (#517) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  pull request labeler (#519) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🚀 Chore


-  update ruff pre-commit and rules (#522) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  Unit tests for coloc (#509) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)


-  small updates to accomodate GWAS Catalog for feb release [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

## v1.1.1 (2024-02-22)
### 🐛 Fix


- \[**finemapping**\] finngen harmonisation (schema and orchestration) (#489) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)


-  update github token variable (#476) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

### 📖 Documentation


-  susie inf method reloacated with the rest of the methods [\@David Ochoa](mailto:dogcaesar@gmail.com)

### ✅ Test


-  increase modularity of test directories (#502) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🏗 Build


- \[**deps-dev**\] bump apache-airflow-providers-google (#496) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump python-semantic-release from 9.0.3 to 9.1.0 (#495) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump yamllint from 1.33.0 to 1.35.1 (#494) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pydoclint from 0.3.8 to 0.4.1 (#497) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest from 7.4.4 to 8.0.1 (#493) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump python-semantic-release from 8.7.0 to 9.0.3 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

### 👷‍♂️ Ci


-  pre-commit autoupdate message (#500) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  pre-commit updates (#499) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  exclude changelog.md from precommit (#479) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**release**\] add action to open pr that triggers release weekly (#474) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

## v1.1.0 (2024-02-08)
### ✨ Feature


- \[**finemapping**\] ingest finngen r10 finemapping w/ airflow (#435) [\@xyg123](mailto:33658607+xyg123@users.noreply.github.com)


- \[**finemapping**\] susie method (#398) [\@Daniel-Considine](mailto:113430683+Daniel-Considine@users.noreply.github.com)


- \[**dataset**\] add `Dataset.filter` to create a new dataset instance with filtered data (#470) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**LD**\] include leads with R2=1 in the ldSet when not found in LDIndex (#453) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 📖 Documentation


- \[**StudyLocusQualityCheck**\] update outdated attributes (#452) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  add gentropy first steps guide (#449) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  pypi badges (#439) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🏗 Build


- \[**deps-dev**\] bump google-cloud-dataproc from 5.8.0 to 5.9.0 (#468) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest-sugar from 0.9.7 to 1.0.0 (#467) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.20.0 to 8.21.0 (#466) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.1.8 to 0.2.0 (#465) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump scikit-learn from 1.3.2 to 1.4.0 (#444) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump pandas from 2.1.4 to 2.2.0 (#443) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipykernel from 6.28.0 to 6.29.0 (#442) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump numpy from 1.26.2 to 1.26.3 (#441) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.19.0 to 8.20.0 (#445) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

### 👷‍♂️ Ci


-  ignore patch versions in dependabot (#462) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🚀 Chore


-  merge dev [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  minor updates to gwas catalog pathnames (#440) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**deps**\] bump codecov/codecov-action from 3 to 4 (#464) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  harmonizing finngen configuration (#454) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**deps**\] bump actions/upload-artifact from 3 to 4 (#447) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump actions/download-artifact from 3 to 4 (#448) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump actions/cache from 3 to 4 (#446) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

## v1.0.0 (2024-01-18)
### ✨ Feature


-  not fail CI if codecov error (#416) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  flex python version to make all 3.10 versions compatible (#406) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**l2g**\] limit l2g predictions to gwas-derived associations (#408) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  remove ukbiobank and overlaps steps (#404) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  application configuration (#386) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**l2g**\] add coloc based features (#400) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  adding logic to flag gwas catalog studies based on curation (#347) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**colocalisation**\] new step runs ecaviar on credible sets (#396) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**eqtl_catalogue**\] study index improvements (#369) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  carma outlier detection method (#281) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**study_locus**\] remove statistics after conditioning from schema (#383) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**l2g**\] add features based on predicted variant consequences (#360) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**pics**\] remove variants from `locus` when PICS cannot be applied (#361) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  Finngen R10 harmonisation and preprocessing (#370) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  semantic release gh action (#354) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  upload release (#353) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  activate release process (#352) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🐛 Fix


-  logo css [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  changelog template (#424) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  deprecate __version__ in __init__.py (#421) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  some of the details of the curation logic needed some fix (#418) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  release action (#417) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  updating finngen sumstats ingestion (#401) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  release build using poetry (#387) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  incorrect parsing of `app_name` in makefile (#367) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**l2g**\] `calculate_feature_missingness_rate` counts features annotated with 0 as incomplete (#364) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

### 📖 Documentation


-  fix step name (#438) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  docs ammendments (#437) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  several enhancements on docs including index and installation (#433) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  release documentation preparation (#432) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  update index.md [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  update README.md (#431) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  gentropy hero image (#430) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  documentation link (#429) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  corrected and added documentation to datasource (#362) [\@Yakov](mailto:yt4@sanger.ac.uk)

### ♻️ Refactor


-  rename genetics_etl_python to gentropy (#422) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  rename otg to gentropy (#405) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### ✅ Test


- \[**study_locus**\] add semantic test for find_overlaps (#407) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  generalise cli test for easier maintainance (#403) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🏗 Build


- \[**deps-dev**\] bump lxml from 4.9.3 to 5.1.0 (#412) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump hail from 0.2.126 to 0.2.127 (#413) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump wandb from 0.16.1 to 0.16.2 (#410) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest from 7.4.3 to 7.4.4 (#415) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocstrings-python from 1.7.5 to 1.8.0 (#414) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pymdown-extensions from 10.5 to 10.7 (#390) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipykernel from 6.27.1 to 6.28.0 (#389) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump apache-airflow-providers-google (#392) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-material from 9.5.2 to 9.5.3 (#391) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-git-committers-plugin-2 from 2.2.2 to 2.2.3 (#393) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.18.1 to 8.19.0 (#377) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-git-revision-date-localized-plugin (#376) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump python-semantic-release from 8.5.1 to 8.7.0 (#375) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mypy from 1.7.1 to 1.8.0 (#374) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump apache-airflow from 2.7.3 to 2.8.0 (#373) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump pyspark from 3.3.3 to 3.3.4 (#358) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.1.7 to 0.1.8 (#341) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump isort from 5.13.1 to 5.13.2 (#342) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump python-semantic-release from 8.3.0 to 8.5.1 (#343) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

### 👷‍♂️ Ci


-  remove unnecessary steps (#428) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  more complex setup with independent jobs (#427) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  pypi and testpypi functionalities (#423) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  reorder commit types in release notes template (#419) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  set codecov default branch to dev (#368) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  new changelog and release notes templates  (#357) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)

### 🚀 Chore


-  improvements to generate 2401 data release (#436) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  tidying up gwas catalog ingestion and process configuration (#426) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  updaing configs to the propsed release folder structure (#425) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  update pre-commit hook versions (#411) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**studylocus**\] rename logABF statistics to logBF (#402) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**deps**\] bump python-semantic-release/python-semantic-release (#388) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  change picsed finngen outputh path (#385) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**deps**\] bump python-semantic-release/python-semantic-release (#372) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  set cluster delete TTL (#379) [\@Kirill Tsukanov](mailto:tskir@users.noreply.github.com)


- \[**study_index**\] change numeric columns from long to integers (#371) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**deps**\] bump python-semantic-release/python-semantic-release (#359) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)

## v0.0.0-rc.1 (2023-12-14)
### 💥 Breaking


-  build and submit process redesigned [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  modular hydra configs and other enhancements [\@David Ochoa](mailto:dogcaesar@gmail.com)

### ✨ Feature


-  release branch (#350) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  yamllint to ensure yaml linting (#338) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  trigger on push (#337) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  track feature missingness rates (#335) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  semantic release automation (#294) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  ruff as formatter (#322) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  track training data and feature importance (#325) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  finngen preprocess prototype (#272) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  add gwas_catalog_preprocess dag (#291) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  Gnomad v4 based variant annotation (#311) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  GWAS Catalog harmonisation prototype (#270) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  add &#39;coalesce&#39; and &#39;repartition&#39; wrappers to &#39;Dataset&#39; (#307) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  Adding cohorts field to study index (#309) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  add prettier as formatter (yaml, json, md, etc.) (#298) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  adding unpublished studies (#290) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  deptry added to handle unused, missing and transitive dependencies (#304) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  add `clump` step (#288) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  ingestion supported for both new and old format of the harmonized GWAS Catalog Summary stats. (#274) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  gitignore .venv file [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  updating summary stats schema and ingtesion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**l2g_gold_standard**\] change `filter_unique_associations` logic [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**overlaps**\] add and test method to transform the overlaps as a square matrix [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  raise error in `from_parquet` when df is empty [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  gcp dataset config [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  datasets config [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  configuration for PICS step [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  new PICS step [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  finetune spark job [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  parametrise autoscaling policy [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  add ability to attach local SSDs [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  populate geneId column [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  join dataframes to add the full study ID information [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  map partial and full study IDs [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  construct study ID based on all appropriate columns [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement summary stats ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement study index ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  eQTL Catalogue main ingestion script [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add eQTL ingestion to the list of steps in DAG [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  hydra full error on dataproc [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  function to retrieve melted ld matrix with resolved variantids [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  type enhancements [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  extract gnomad ld matrix slice [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  local chain file [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  variant annotation within class [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gnomAD LD datasource contains path [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  enable autoscaling [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**variant_annotation**\] include variant_annotation step as part of the preprocessing dag [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] include ldindex step as part of the preprocessing dag [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  implement Preprocess DAG [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**airflow**\] remove cluster autodeletion [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update gcp sdk version to 452 and airflow image to airflow [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update gcp sdk version to 452 and airflow image to airflow [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  switch back to non root after gcp installation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  change airflow image to `airflow [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  display class name in docs and cleanup docs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  apply pydoclint pre commit to `src` only [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `make check` has two tiers of docstrings linting [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  configure pydoclint to require return docstring when returning nothing [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  configure pydoclint to check all functions have args in their docstrings [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  configure pydoclint to allow docstrings in init methods [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  configure pydoclint to require type signatures for all functions [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `pydoclint` to make check rule [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add docs and reorganise modules [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] start session inside step instance [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adding notebooks for finngen and ld matrix [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**l2g**\] working session interpolation with yaml [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] pass extended_spark_conf as dict, not sparkconf object [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  start l2g step with custom spark config [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  migrate to LocalExecutor [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gitkeep config directory [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  dependabot to monitor airflow docker [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gitkeep dags folder as well [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  streamline docker configuration [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**study_index**\] harmonise all study indices under common path [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] l2gprediction.from_study_locus generates features instead of reading them [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  implement submit_pig_job in common_airflow [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  additional changes for dag_genetics_etl_gcp [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  additional changes for airflow_common [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add the common module for Airflow dags [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**l2g**\] add sources to gold standard schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] new module to capture gold standards parsing logic [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] add evaluate flag to trainer [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**intervals**\] create Interval.from_source [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**intervals**\] abstract logic from sources wip [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**intervals**\] prototype of collect_interval_data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  added data bio [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


- \[**v2g**\] remove  from schema - no longer used to write dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] read files recursively [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**studylocus**\] change `unique_lead_tag_variants` to `unique_variants_in_locus` + test [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**github**\] check toml validity on cicd [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**dependabot**\] preformat commit messages [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**github**\] check toml validity on cicd [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  dependabot for poetry/pip and github actions [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  changing the action [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  bump poetry packages [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  change catalog_study_locus path under common study_locus [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add l2g_benchmark.ipynb [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  install jupyter in dev cluster [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  finalise summary stats ingestion logic [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  stub for running summary stats ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement FinnGen summary stats ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  port FinnGen study index ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  separate Preprocess code [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update Makefile to upload Preprocess files [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add logic to predict step [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add fm schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add feature list as configuration [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  improve LocusToGeneModel definition and usage [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  wip work with real data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  etl runs, tests fail [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  _convert_from_wide_to_long with spark [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  convert the distance features to long format [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add utils to parse schema from pandas [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add common functions to melt and pivot dfs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  checkpoint, cli works [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove dataset configs, will be handled with struct configs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  l2g step configuration step accommodated in the general cli [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename datasets schema to _schema, organise config classes, and progress with cli [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  implement config for L2GStep [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  checkpoint [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  create mock coloc, study locus and other minor fixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add geneId to studies schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  calculate coloc, naive distance features as class methods [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  l2g targets skeleton [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  dont filter credible set by default [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  enhancements around pics and annotate credible set [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  prevent precommit issues with imports [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  flexible window set up for locus collection [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  handling studyLocus with existing ldSet [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  ld annotation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  deprecate function [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  various pics fixes [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  generalizing ancestry mapping [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**gcsc**\] functionality for the google cloud storage connector [\@Daniel Considine](mailto:dc16@sanger.ac.uk)


-  intervals tests with several bugfixes [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  moving update logic to a shell file [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  moving update logic to a shell file [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding notebook to update GWAS Catalog data on GCP [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  externalise data sources [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  add common spark testing config [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] add `extended_conf` parameter [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  ensure schema columns are camelcase [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  incomplete renaming of tags [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  move pytest instructions to project level [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adding xdist function in github action [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  implementation of xdist [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  rename credibleSet to locus [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  docs build is now tested! [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  convert assign_study_locus_id to a class method [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove redundant assign_study_locus_id [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move `get_study_locus_id` inside studylocus [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move `get_study_locus_id` inside studylocus [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  reducing data by clustering [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  purely window based clustering [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  testing window based clumping in real data [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding exclusion filter to sumstats [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**makefile**\] create rule to format, test, and build docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**pre-commit-hooks**\] deprecate flake8 and add ruff [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**linter**\] deprecate flake8 and set up ruff in project and vscode settings [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  wokflow is now triggered manually (rename required) [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**validate_schema**\] only compare field names when looking for unexpected extra fields [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add skeleton for overlaps step [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**_align_overlapping_tags**\] nest stats [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**study_locus**\] bring more stats columns in `find_overlaps` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**overlaps_schema**\] add pvalue, beta and nest statistics [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  first dag [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  first dag [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**Makefile**\] add [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**cluster_init**\] uninstall otgenetics if exists [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**Workflow**\] add args for ssd and disk size [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  change repartition strategy before aggregation [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**Workflow**\] assign 1000GB to disk boot size [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] assign executors dinamically based on resources [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  feat(workflow_template) [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add persist/unpersist methods to `dataset` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add kwargs compatibility to `from_parquet` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rewrite `TestValidateSchema` to follow new paradigm [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove `from_parquet` and add `_get_schema` to every Dataset child class [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**Dataset**\] add abstract class to get schema and invoke this inside from_parquet [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  filter credibleSet to only include variants from the 95% set [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] partition by chromosome [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] partition by chromosome [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt gwas_catalog module [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `ldSet` to study_locus schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rewrite `LDAnnotator` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  refactor `get_gnomad_ancestry_sample_sizes` to `get_gnomad_population_structure` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `_aggregate_ld_index_across_populations` [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


- \[**_create_ldindex_for_population**\] add population_id [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  deprecate old ld_index dataset [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] add `resolve_variant_indices` [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  wip new ldset dataset [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  write reference agnostic liftover function [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  change r2 threshold to retrieve all variants above .2 and apply filter by .5 on r2overall [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**_variant_coordinates_in_ldindex**\] replace groupby by select [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `get_study_locus_id` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix ldclumping, tests, and docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  function to clump a summary statistics object [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding main logic [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  add ld_matrix step [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ld**\] remove unnecessary grouping - variants are no longer duplicated [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] remove ambiguous variants [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  update ingestion fields [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update input/output paths [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add create-dev-cluster rule to makefile (#94) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  ukbiobank sample test data [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  ukbiobank components [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add UKBiobank config yaml [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add UKBiobank step ID [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add UKBiobank inputs and outputs [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add UKBiobankStepConfig class [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add StudyIndexUKBiobank class [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  ukbiobank study ingestion [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  replace hardcoded parameters with cutomisable ones [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  run the code from the version specific path [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  upload code to a version specific path [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  extract `order_array_of_structs_by_field`, handle nulls and test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  order output of `_variant_coordinates_in_ldindex` and tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  extract `get_ld_annotated_assocs_for_population` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  extract methods from `variants_in_ld_in_gnomad_pop` (passes) [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rewrite FinnGenStep to follow the new logic [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement the new StudyIndexFinnGen class [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add step to DAG [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update FinnGen ingestion code in line with the current state of the main branch [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  pass configuration values through FinnGenStepConfig [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  pass FinnGen configuration through my_finngen.yaml [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rewrite EFO mapping in pure PySpark [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  validate FinnGen study table against schema [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement EFO mapping [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  write FinnGen study table ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  added run script for FinnGen ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  fix PICS fine mapping function [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  deprecate StudyLocus._is_in_credset [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  improved flattening of nested structures [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  new annotate_credible_sets function and tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  changes on the tests sessions with the hope to speed up tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  update dependencies to GCP 2.1 image version [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gpt commit summariser [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fixes associated with study locus tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  first slimmed variant annotation version with partial tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  docs badge as link [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  README minor updates [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  change  not to return False when df is of size 1 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add check for duplicated field to `validate_schema` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add support and tests for nested data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merge with remote branch [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add support and tests for nested data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  added flatten_schema function and test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add type checking to validate_schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  run tests on main when push (or merge) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  run tests on main when push (or merge) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  run tests on main when push (or merge) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  redefine validate_schema to avoid nullability issues [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  redefine `validate_schema` to avoid nullability issues [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  working version of dataproc workflow [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  add logic to predict step [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add fm schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  first working version of google workflow [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  hail session functionality restored [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  include pyright typechecking in vscode (no precommit) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  sumstats ingestion added [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  step to preprocess sumstats added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  half-cooked submission using dataproc workflows [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  vscode isort on save [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  updating summary stats ingestion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  yaml config for gene index [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix CLI to work with hydra [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  intervals to v2g refactor and test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove gcfs dependency [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  bugfixes associated with increased study index coverage [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  add feature list as configuration [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  improve LocusToGeneModel definition and usage [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  wip work with real data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adding p-value filter for summary stats [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding summary stats dataset [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  several fixes linked to increased test coverage [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  etl runs, tests fail [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  working hydra config with optional external yaml [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  _convert_from_wide_to_long with spark [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  convert the distance features to long format [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add utils to parse schema from pandas [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add common functions to melt and pivot dfs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  checkpoint, cli works [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove dataset configs, will be handled with struct configs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  l2g step configuration step accommodated in the general cli [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  precompute LD index step [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  rename datasets schema to _schema, organise config classes, and progress with cli [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  ld_clumping [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  r are combined by weighted mean [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pics refactor [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  ld annotation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gwas catalog splitter [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  implement config for L2GStep [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  contributors list [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  checkpoint [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  create mock coloc, study locus and other minor fixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add geneId to studies schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  calculate coloc, naive distance features as class methods [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  distance to TSS v2g feature [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  schemas are now displayed in documentation! [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  default p-value threshold [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  minor improvements in docs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  major changes on association parsing [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  graph based clumping [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  gs checkpoint [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  function to install custom jars [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  l2g targets skeleton [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor changes in the parser [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gwascat study ingestion preliminary version [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  incorporating iteration including colocalisation steps [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  exploring tests with dbldatagen [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  more step config less class [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  not tested va, vi and v2g [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  directory name changed [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merging main into il-v2g-distance [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  cli working! [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  first prototype of CLI using do_hydra [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  poetry lock updated [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove `phenotype_id_gene` dependency [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt coloc code to newer datasets [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merge [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  reverting flake8 to 5.0.4 [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  feat [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  ignore docstrings from private functions in documentation [\@David](mailto:ochoa@ebi.ac.uk)


-  plugin not to automatically handle required pages [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  filter v2g based on biotypes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  generate v2g independently and read from generated intervals [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  compute distance to tss v2g functions [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  v2g schema v1.0 added [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add polyphen, plof, and sift to v2g [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  trialing codecov coverage threshold [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  integrate interval parsers in v2g model [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rearrange interval scripts into vep [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add skeleton of vep processing [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  ipython support [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  token added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  trigger testing on push [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  extra config for codecov [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  enhanced codecov options [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  codecov integration [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  spark namespace to be reused in doctests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  validate studies schema [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  update actions to handle groups [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  license added [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  mkdocs github action [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  first version of mkdocs site [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  generate study table for GWAS Catalog [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  docs support [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  non-nullable fields required in validated df [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  intervals DataFrames are now validated [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  new mirrors-mypy precommit version [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  intervals schema added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  function to validate dataframe schema [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  handling schemas on read parquet [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding effect harmonization to gwas ingestion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding functions to effect harmonizataion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  gwas association deduplication by minor allele frequency [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  debugging in vscode [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  ignoring __pycache__ [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  coverage added to dev environment [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  increased functionality of the gwas catalog ingestion + tests [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  mapping GWAS Catalog variants to GnomAD3 [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding GWAS Catalog association ingest script [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  black version bumped [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding variant annotation [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  updated dependencies [\@Davvid Ochoa](mailto:dogcaesar@gmail.com)


-  Adding parsers for intervals dataset. [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**pre-commit**\] commitlint hook added [\@Davvid Ochoa](mailto:dogcaesar@gmail.com)

### 🐛 Fix


-  unnecessary option (#351) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  several issues (#349) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  github token (#348) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  release actions fixes (#344) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**clump**\] read input files recursively (#292) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  correct and test study splitter when subStudyDescription is the same (#289) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  making standard_error column optional (#286) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  proper parsing of gwas catalog study accession from filename (#282) [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  local SSD initialisation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  multiply standard error by zscore in `calculate_confidence_interval` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g_gold_standard**\] fix logic in `remove_false_negatives` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  wrong lines removed [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  change definition of negative l2g evidence [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  studies sample filename [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  typo in position field name [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  manually specify schema for eQTL Catalogue summary stats [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  cast nSamples as long [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  populating publicationDate [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  include header when reading the study index [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  do not initialise session in the main class [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  name of EqtlCatalogueStep class [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  eqtl_catalogue path in docs [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update class names [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  do not partition by chromosome for QTL studies [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  coalesce variantid to assign a studylocusid [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  persist raw gwascat associations to return consistent results [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  gnomad paths are not necessary after #233 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert testing changes [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gwas_catalog**\] clump associations, remove hail and style fixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  extract config in root when we install deps on cluster [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  wrong python file uri in airflow [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  commiting example block matrix [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**session**\] add `mode_overwrite` default to configs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert 69cb5c13fba97fc0c3a73f51a306f74c099e5d42 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  change chain location to gcp [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  no longer installed dependencies (too long) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  removing unnecessary print statement [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  revert changes in spark fixture [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  un-comment rows in test_session.py [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  fix typo [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  phasing out session initialization [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  remove default session from steps [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  python_module_path is not built inside `submit_pyspark_job` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] hail config was not set unless extended_spark_conf was provided [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] remove default session in the step [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  default_factory now takes lambda [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  reverting field from dataclass [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**config**\] specify start_hail for all configs to avoid recursive interpolation [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] config to pick up `start_hail` flag [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  different syntax for relative import [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  allow relative imports [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rename method in FinnGenSummaryStats [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**airflow**\] job args are list of strings [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**pre-commit**\] ignore d107 rule in pydocstyle due to clash with pydoclint [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**config**\] store main config as `default_config` to deduplicate from yaml [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**docs**\] add python handler to exclude private methods [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] set default session [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**dag**\] uncomment lines [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] add `wandb` to main dependencies [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  step bugfixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  step bugfixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] bugfix predict step [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] drop `sources` before conversion to feature matrix [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] bugfixes to run step [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**features**\] remove deprecated feature generation methods [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  pass cluster_name to install_dependencies [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  install_dependencies syntax [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  common DAG initialisation parameters [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  set DAG owner to fix assertion error [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  variable name [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  delete deprecated files [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor fix [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**intervals**\] import within Intervals.from_source to avoid circular dependencies [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  updated OT doc [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  added to class docstring [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


- \[**docs**\] update link to roadmap and contributing sections [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**docs**\] correct schemas path for gene and study index [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**v2g**\] indicate schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**v2g**\] indicate schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**intervals**\] remove `position` from v2g data [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**liftover**\] use gcfs to download chain file when provided gcs paths [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**v2g**\] read intervals passing spark session [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] `get_distance_to_tss` returns distance instead of position [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gene_index**\] add `obsoleteSymbols` and simplify schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gene_index**\] add `approvedName` `approvedSymbol` `biotype` to `as_gene_index` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gene_index**\] typo in gene_index output dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gene_index**\] add `obsoleteSymbols` and simplify schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gene_index**\] add `approvedName` `approvedSymbol` `biotype` to `as_gene_index` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**gene_index**\] typo in gene_index output dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**v2g**\] change `id` to `variantId` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**v2g**\] convert biotypes to python list [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  correct thurman typo [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] revert recursive lookup and use kwargs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  relative links instead of absolute [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  trying to specify python version [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  trying to force python version [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**test_cross_validate**\] test learning rate in params [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**test_train**\] fix problem of task failing during a barrier stage [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  conflict with thurman [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  incorrect filename [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**l2g**\] set correct output column in `evaluate` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove unnecessary lead variant id from feature matrix [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  comment out coloc factory [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  do not upload preprocess as part of this PR [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove redefining get_schema [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  move Preprocess/SummaryStats changes to the old location [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove Preprocess/StudyIndex changes [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update schema name after upstream changes [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  l2g tests fix, more samples added so that splitting doesnt fail [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove typing issues [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  tests pass [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  regenerate lock file [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix study_locus_overlap test - all passing [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  l2g step uses common etl session [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove `data` from .gitignore [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move `schemas` and `data` to root folder [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  bring back main config for package bundling [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  labelling tags with null posterior as false instead of null [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  formatting [\@Daniel Considine](mailto:dc16@sanger.ac.uk)


-  removing some unneccesary files [\@Daniel Considine](mailto:dc16@sanger.ac.uk)


-  schema issues due to when condition [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  removing gsutil cp dags/* from build [\@Daniel Considine](mailto:dc16@sanger.ac.uk)


-  update makefile [\@Daniel Considine](mailto:dc16@sanger.ac.uk)


-  some studies just don&#39;t have population data [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  empty array of unknown type cannot be created and saved. Fixed [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  fixing column selection [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  cleaning clumping [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  the studylocus returned by window based clumping has qc column [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  typo in length [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  dealing with schemas pre and post PICS [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  unnecessary test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  useless test [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**ids**\] correcting version numbers for pull request [\@Daniel Considine](mailto:dc16@sanger.ac.uk)


-  adapt pics test to newer data model [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  un-commented lines to actually fetch data [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  updating GWAS Catalog input files [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  doctest, test [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  doctest [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  test fail [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  update coloc logic to new fields [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update coloc logic to new fields [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  column pattern can include numbers [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  use vanilla spark instead of our custom session [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adding sample data for testing [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  merge main + fixing tests [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  minor udpates [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  renamed columns [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  properly use new hooks from mkdocs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merging with main [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adjusting dataset initialization [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  updated action to install docs in testing environment [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  restore missing file [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  session setup preventing to run tests in different environments [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  failing airflow tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missing import [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  incorrect extension name [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  correct extensions name [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  typo in check rule [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  apply ruff suggestions [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  mess caused when incorrectly pushing my branch [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  step minor bugfixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt tests to latest changes in the overlaps schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  unnecessary CHANGELOG [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  failed dependency [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**studylocus**\] assign hashed studylocusid after study splitting [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**studylocusgwascatalog**\] use [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove repartiitoning and adjust aggregation to a single col - working [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  lower memory threshold [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert addig chromosome to join to resolve variants [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt schema definition in tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt schema definition in tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  correct formula in `_pics_standard_deviation` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  typo in pics std calculation as defined by PMID [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt `test_annotate_credible_sets` mock data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix `_qc_unresolved_ld` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  correct ld_index_path [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor bugs to success [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  set ld_index fields to non nullable [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor bugs for a successful run [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**_transpose_ld_matrix**\] bugfix and test [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] minor bug fixes to run the new step [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] minor bug fixes to run the new step [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**_variant_coordinates_in_ldindex**\] order variant_coordinates df just by idx and not chromosome [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  start index at 0 in _variant_coordinates_in_ldindex [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  updating tests [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  update expectation in `test__finemap` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  comply with studyLocus requirements [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  avoid empty credible set (#3016) [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  pvalueMantissa as float in summary stats [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  spark does not like functions in group bys [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  stacking avatars in docs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typo in docstring [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  implement suggestions from code review [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  revert accidentally removed link [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  set pvalue text to null if no mapping is available [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert removing subStudyDescription from the schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  use studyId instead of projectId as primary key [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  change substudy separator to &#34;/&#34; to fix efo parsing [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  resolve trait accounting for null p value texts [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  updated StudyIndexUKBiobank spark efficiency [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  update gwascat projectid to gcst [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  updated StudyIndexUKBiobank [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  incorrect parsing of study description [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  right image in the workflow [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  update conftest.py read.csv [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  update gcp.yaml spacing [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  update gcp.yaml spacing [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  correct sumstats url [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  decrease partitions by just coalescing [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  variable interpolation in workflow_template.py [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  reorganise workflow_template.py to make pytest and argparse work together [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  typos [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  revert accidental DAG changes [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  reduce num partitions to fix job failure [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  handle empty credibleSet in `annotate_credible_sets` + test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  control none and add test for _finemap + refactor [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  do not control credibleSet size for applying finemap udf [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove use of aliases to ensure study index is updated [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt `finemap` to handle nulls + test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove null elements in credibleSet [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  handle null credibleSet as empty arrays [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  define finemap udf schema from studylocus [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add missing columns to `_variant_coordinates_in_ldindex` + new test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update indices column names in `get_ld_annotated_assocs_for_population` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  populate projectId and studyType constant value columns [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  configuration target for FinnGenStepConfig [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update lit column init to agree with `validate_df_schema` behaviour [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rewrite JSON ingestion with RDD and remove unnecessary dependencies [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  configuration variable name [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  several errors found during debugging [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  resolve problems with Pyenv/Poetry installation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  activate Poetry shell when setting up the dev environment [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update `ld_index_template` according to new location [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert changes in `.vscode` [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  calculate r2 before applying weighting function [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  restore .gitignore [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  flatten_schema result test to pyspark 3.1 schema convention [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert to life pre-gpt [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  altering order of columns to pass validation (provisional hack) [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  cascading effects in other schemas [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove nested filter inside the window in [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  handle duplicated chrom in v2g generation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `_annotate_discovery_sample_sizes` drop default fields before join [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `_annotate_ancestries` drop default fields before join [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `_annotate_sumstats_info` drop duplicated columns before join and coalesce [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  order ld index by idx and unpersist data [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  correct attribute names for ld indices [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  join `_variant_coordinates_in_ldindex` on `variantId` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move rsId and concordance check outside the filter function [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  l2g tests fix, more samples added so that splitting doesnt fail [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update configure and gitignore [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  type issue [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typing issue [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typing issue fixed [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typing and tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  right populations in config [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gnomad LD populations updated [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  blocking issues in variant_annotation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  blocking issues in ld_index [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typing issue [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  tests working again [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missing f.lit [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  extensive fixes accross all codebase [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missing chain added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typo in config [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove typing issues [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fixing sumstats column nullabitlity [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  clearing up validation [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  steps as dataclasses [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  incorrect config file [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fixing test allowing nullable column [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  rename json directory to prevent conflicts [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  tests pass [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  regenerate lock file [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix study_locus_overlap test - all passing [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fixing doctest [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  merging with do_hydra [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  l2g step uses common etl session [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  addressing various comments [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missing dependency for testing [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missed dbldatagen dependency [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pyupgrade to stop messing with typecheck [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  operative pytest again [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  wrong import [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  df not considered [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove `data` from .gitignore [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move `schemas` and `data` to root folder [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  consolidate path changes [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  bring back main config for package bundling [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  using normalise column [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  bug with config field [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  exclude unneccessary content [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typo in the schema [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  import fix [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  finalizing claping logic [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  type fixes [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  issues about clumping [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  merge conflicts [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merge conflicts resolved [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  incorrect super statement [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  wrong paths [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix normalisation of the inverse distance values [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor bugfixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  schema fix [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  dropping pyupgrade due to problems with hydra compatibility in Union type [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  comment out filter on chrom 22 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove repartition step in intevals df [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix tests data definition [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  revert unwanted change [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  correct for nullable fields in `studies.json [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  write gwas studies in overwrite mode [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  correct column names of `mock_rsid_filter` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  typo in mock data in mock_allele_columns [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  camel case associations dataset [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  structure dev dependencies [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fetch-depth=0 [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  depth=0 as suggested by warning [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  module needs to be installed for mkdocstring [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  docs gh action refactor [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  variable name changed [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**pr comments**\] abstracting some of the hardcoded values, changing z-score calculation [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  clash between vscode and pretty-format-json [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  clash between black and pretty format json [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**config**\] parametrizing target index [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**schema**\] camel casing column names [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  updates for new gene index [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  flake FS002 issue resolved [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  copy target index into the release playground bucket [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  fixed imports in intervals [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  might be required [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pytest gh actions now adds coverage [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  more fixes to gh action [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pytest gh action [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gwas ingestion adapted to new structure [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  testing strategy moved to github actions [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pytest hook fixed [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  python version and dependencies [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding hail to dependency + moving gcsfs to production with the same version as on pyspark [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  fixing python and spark version [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  Missing interval parser added. [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  commitlint hook was not working [\@Davvid Ochoa](mailto:dogcaesar@gmail.com)

### 📖 Documentation


-  finngen description v1 (#345) [\@Yakov](mailto:yt4@sanger.ac.uk)


-  minify plugin removed to prevent clash in local development (#284) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  pics step [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  add automatically generated docs [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update contributing checklist [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update running instructions [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  generalizing GnomAD class documentation [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  add rudimentary documentation on DAGs and autoscaling [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rewrite section to add user ID [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove unnecessary comment which is replaced by a note [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  set correct user ID to fix file access issues [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  clarify parameter specific for CeleryExecutor [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  do not hardcode name of project [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  fix list and code block formatting [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  fix contributing checklist formatting [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  streamline Airflow documentation style and tone [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  l2g step title [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  add session to attributes list for all steps [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] add step documentation [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] add titles to files [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  study_locus schema at the end [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  create and organise development section [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update index to match docs update [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  fix command to start docs server [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  make development a separate section [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add instructions for running Airflow [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  create and organise development section [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rename all instances of Thurnman to Thurman [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  structural changes and data source images [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  several fixes in docstrings [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  window_length no longer defines the collect locus [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  enhance locus_collect column [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  more meaningful message [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  added descriptions [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove links pointing nowhere to fix mkdocs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix incorrect reference [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix relative link in `contributing.md` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  improve overlaps docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  improve documentation on the overall process [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  page for window based clumping [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove the now unneeded file from utils [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  reorganise troubleshooting section [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update index with a separate troubleshooting section [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  move troubleshooting information from the contributing guide [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update documentation on building and testing [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  improve `_annotate_discovery_sample_sizes` docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  updated java version text [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  finngen study index dataset duplicated in docs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  finngen study index dataset duplicated in docs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  Update ukbiobank.py docstring [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  compile the new contributing section [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add contributing information to index [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  move technical information out of README [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add need to install java version 8 [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  fix link [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix fixes [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  update index.md [\@buniello](mailto:30833755+buniello@users.noreply.github.com)


-  reference renamed to components based on buniellos request [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  correct version naming example [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  amend documentation to make clear how versioning works [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  missing docs for finger step added [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  rewrite README with running instructions [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add documentation on StudyIndexFinnGen [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add troubleshooting instructions for Pyenv/Poetry [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  simplify instructions for development environment [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  docs badge added [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  fixing actions [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fixing actions [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fixing actions [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix to prevent docs crashing [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix doc [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  explanation improved [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missing docstring [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gwas catalog step [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  missing ld clumped [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typo [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  including titles in pages [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  improved description [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  extra help to understand current configs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  address david comment on verbose docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  correct reference to coloc modules [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update summary of logic [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  added docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  added v2g docs page [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  scafolding required for variant docs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  generate_variant_annotation documentation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  removing old docsc (#36) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  rounding numbers for safety [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  first pyspark function example [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  non-spark function doctest [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  doctest functionality added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  openblas and lapack libraries required for scipy [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  improvements on index page and roadmap [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  license added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  better docs for mkdocs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  function docstring [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  schema functions description [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding badges to README [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding badges to README [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding documenation for gwas ingestion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  not necessary [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  adding documentation for Intervals and Variant annotation [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### 🎨 Style


-  docstring for eQTL Catalogue summary stats ingestion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  move session config to top [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use uniform lists in YAML [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  complete typing and docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename top module variables to upper case [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  fix docstring linting issues [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix docstring linting issues [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**config**\] rename `my_config` to `config` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  simplify task names [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**intervals**\] rename individual reading methods to common name [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  self code review [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**study_locus**\] remove unnecessary logic [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  import row [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  reorganise individual function calls into a concatenated chain of calls [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  uniform comments in gcp.yaml [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  reorganise configuration in line with the new structure [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rename ld indices location to directory and no extension [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  using fstring for concatenation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  validate variant inputs [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)

### ♻️ Refactor


-  stop inheriting datasets in parsers (#313) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  removing odds ratio, and confidence intervals from the schema [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**gold_standard**\] move logic to refine gold standards to `L2GGoldStandard` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  turn `OpenTargetsL2GGoldStandard` into class methods [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move hardcoded values to constants [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  modularise logic for gold standards [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move gene ID joining into the study index class [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  reorganise study index ingestion for readability [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update eQTL study index import [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  `get_ld_variants` to return a df or none [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  final solution using defaults [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use defaults override for configuration [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove config store [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove legacy start_hail attributes [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  explicitly set spark_uri for local session [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove repeatedly setting default [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  move session default to config.yaml [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  gnomad ld test refactored [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  changes specific to FinnGen [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  simplify reading YAML config [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use generate_dag() to simplify ETL DAG [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use generate_dag() to simplify Preprocess DAG [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement generate_dag() to further simplify layout [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use submit_step() to simplify the main DAG [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add submit_step() as a common Airflow routine [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  make step docstrings uniform [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  change run() to __post_init__() [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove step.run() from cli.py [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rename all steps without &#34;my_&#34; [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update docs and remaining classes [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise VariantIndexStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise VariantAnnotationStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise V2GStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise UKBiobankStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise StudyLocusOverlapStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise LDIndexStepConfig [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise GWASCatalogSumstatsPreprocessStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise GWASCatalogStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise GeneIndexStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise FinnGenStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  finalise ColocalisationStep [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove config store entries for steps [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add _target_ to YAML configuration files [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove defaults + comments from YAML config files [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove *StepConfig from documentation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**feature**\] move `L2GFeature` inside dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add install_dependencies to common_airflow [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use a single shared cluster for all tasks [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  subgroup and DAG definition [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  submitting a PySpark job [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove redundant step to install dependencies [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  unify cluster deletion [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  unify cluster creation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  reconcile changes with main [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**intervals**\] change input structure to dict [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] remove cols parameter in filter_by_variant_df [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**liftover**\] download chain file with google cloud api and not gcsfs [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] remove `position` from v2g data [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] remove `label`from `get_sift_v2g` and improve docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] remove `label`from `get_polyphen_v2g` and improve docs [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_annotation**\] `get_most_severe_vep_v2g` doesn&#39;t parse csq and `label`is removed [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**studylocus**\] simplify `unique_variants_in_locus` and improve testing [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variantindex**\] add logic to filter for variants to `VariantIndex.from_variant_annotation` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**variant_index**\] checkpoint [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  convert `L2GFeatureMatrix.fill_na` to class method [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  use featurematrix as input to predict and fit [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  simplifying clumping function call [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  move L2GFeature back to feature_factory to avoid circular import [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move L2GFeature inside datasets [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename classifier to estimator + improvements [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add &#34;add_pipeline_stage&#34; to nested cv [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  relocate schemas inside dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `_get_reverse_complement` returns null if allele is nonsensical [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  generalise ld annotation to study locus level [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**pics**\] remove unnecessary absolute value for r2 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  tidying one function [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  remove notebook [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  generalizing clumping funcion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  rename left and right studyLocusId to camel case [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename colocalisation fields to follow camelcase [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename  to [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor changes to ld [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `clump` and `unique_lead_tag_variants` to use the ldSet [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rewrite `StudyLocus.annotate_ld` to accommodate new logic [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move iteration over pops to `from_gnomad` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  the logic in calculate_confidence_interval_for_summary_statistics is moved [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**ld_annotation_by_locus_ancestry**\] use select instead of drop for intelligibility [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  create credibleSet according to full schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ld_annotation_by_locus_ancestry**\] get unique pops [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  some minor issues sorted out around summary statistics [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


- \[**ldindex**\] remove unnecessary nullability check [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  remoce subStudyDescription from associations dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  deprecate _is_in_credset [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove chromosome from is_in_credset function [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  drop redundants in tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move L2GFeature back to feature_factory to avoid circular import [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move L2GFeature inside datasets [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename classifier to estimator + improvements [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add &#34;add_pipeline_stage&#34; to nested cv [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  relocate schemas inside dataset [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  `_get_reverse_complement` returns null if allele is nonsensical [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  exploring custom jar integration [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  review biotype filter [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename to `geneFromPhenotypeId` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename `coloc_metadata` to `utils` to add `extract_credible_sets` fun [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  state private methods and rename `coloc_utils` folder [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  simplify logic in run_coloc [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  find_all_vs_all_overlapping_signals processes a df [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  export indepdently, part by chrom and ordered by pos [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  extract the logic to read data from the main function to reuse the dfs [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  convert parameters to dataframes, not paths [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  bigger machine, added score to jung, optimisations [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt config and remove run_intervals.py [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  default maximum window for liftover set to 0 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  minor improvements [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  apply review suggestions [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  optimise `test_gwas_process_assoc` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  reorder `process_associations` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adapt `map_variants` to variant schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  reorder `ingest_gwas_catalog_studies` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  slim `extract_discovery_sample_sizes` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  optimise study sumstats annotation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  optimise `parse_efos` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  use select statements in `get_sumstats_location` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merging main branch [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### ⚡️ Performance


-  persist data after first aggregation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  persist data after first aggregation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  follow hail spark context guidelines [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  set number of shuffle partitions to 10_000 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  transpose ld_matrix just before aggregating [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  repartition each ldindex before unioning [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add chromosome to join to resolve variants [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  do not enforce nullability status [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  replace withColumn calls with a select [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)

### ✅ Test


-  Improvements to `test_dataset` and `test_clump_step` (#312) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  failing doctest in different python version (#320) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**l2g_gold_standard**\] add `test_remove_false_negatives` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test_filter_unique_associations` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  testing for `process_gene_interactions` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test_expand_gold_standard_with_negatives_same_positives` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix and test logic in `expand_gold_standard_with_negatives` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test_parse_positive_curation` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for eQTL Catalogue summary stats [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add test for eQTL Catalogue study index [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add sample eQTL Catalogue summary stats [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add sample eQTL Catalogue studies [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add conftests for eQTL Catalogue [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  refactor test_gnomad_ld [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for empty ld_slice wip [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adding test for variant resolved LD function [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  rescue missing test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fixing hail initialization [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  add hail config to spark session to fix `test_hail_configuration` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adjusting environment [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  add `hail_home` fixture [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  a better way to test import of DAGs [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use a different way to amend path [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  re-enable and fix test_no_import_errors [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**l2g**\] add `test_get_tss_distance_features` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test_get_coloc_features` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**schemas**\] add doctest in _get_spark_schema_from_pandas_df [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] add test_l2g_gold_standard [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fixing clumping test [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  update the tests [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update FinnGen summary stats data sample [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add tests for FinnGenSummaryStats [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update existing tests [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add test for `train` method [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for cross validation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add tests for l2g datasets [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for `add_pipeline_stage` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  text fixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  the expectation is that an empty locus can result in none [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  qc_nopopulation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix tests [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  adding test for gwas catalog study ingestion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  additional tests around finemapping [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  duplicated test filename [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  correct sample file [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix sample dataset used [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  isolated pyspark for _prune_peak function [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  chromosome missing in fixture [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding tests [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  added `test_overlapping_peaks` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  added `test_study_locus_overlap_from_associations` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `TestLDAnnotator` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test__resolve_variant_indices` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update ldindex test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add doctest to `_aggregate_ld_index_across_populations` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `calculate_confidence_interval` doctest [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test__finemap` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update failing test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  more clarity in _collect_clump test [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  _filter_leads functions doctest [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  testing for window based clumping function [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  update test sample data [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update tests [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add UKBiobank study index test [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add UKBiobank study configuration test [\@hlnicholls](mailto:53306752+hlnicholls@users.noreply.github.com)


-  add `test_finemap_pipeline` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  create non zero p values [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `test_finemap_null_r2` (fails) [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  improve ld tests [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  fix FinnGen study test [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  test ingestion from source for StudyIndexFinnGen [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement sample FinnGen study data fixture [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  prepare sample FinnGen study data [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  test schema compliance for StudyIndexFinnGen [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  implement mock study index fixture for FinnGen [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add `test_variants_in_ld_in_gnomad_pop` (hail misconfiguration error) [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test_variant_coordinates_in_ldindex (passes) [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  doctest dataframe ordering unchanged [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  map_to_variant_annotation_variants [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  study locus gwas catalog from source [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  added [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  is in credset function [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  added `test_validate_schema_different_datatype` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `TestValidateSchema` suite [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for `train` method [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for cross validation [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add tests for l2g datasets [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add test for `add_pipeline_stage` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  text fixes [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  convert_odds_ratio_to_beta doctest [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding more tests to summary statistics [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  gene_index step added [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  concatenate substudy description [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  qc_all in study-locus [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  more qc tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  qc incomplete mapping [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  qc unmapped variants [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gnomad position to ensembl [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  helpers [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  session [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  splitter [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  additional coverage in ld_index and gene_index datasets [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  additional tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pvalue normalisation [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  failing test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pvalue parser [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  unnecessary tests [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  palindromic alleles [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding tests for summary stats ingestion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  pytest syntax complies with flake [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  ld clumping test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  archive deprecated tests and ignore them [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  added tests for distance features [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add validation step [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  refactor of testing suite [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  added test suite for v2g evidence [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  test only running on PRs again [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  revert change [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adapted to camelcase variables [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  checking all schemas are valid [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pytest added to vscode configuration [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  nicer outputs with pytest-sugar [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  adding more tests on v2d [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)

### 🏗 Build


- \[**deps-dev**\] bump ipykernel from 6.26.0 to 6.27.1 (#332) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest-xdist from 3.4.0 to 3.5.0 (#333) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.1.6 to 0.1.7 (#331) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump google-cloud-dataproc from 5.7.0 to 5.8.0 (#330) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump typing-extensions from 4.8.0 to 4.9.0 (#317) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump numpy from 1.26.1 to 1.26.2 (#314) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pre-commit from 3.5.0 to 3.6.0 (#316) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-material from 9.4.14 to 9.5.2 (#324) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump wandb from 0.16.0 to 0.16.1 (#315) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump apache-airflow-providers-google (#302) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-git-committers-plugin-2 from 1.2.0 to 2.2.2 (#303) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pymdown-extensions from 10.3.1 to 10.5 (#301) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump scipy from 1.11.3 to 1.11.4 (#299) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-material from 9.4.10 to 9.4.14 (#300) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mypy from 1.7.0 to 1.7.1 (#278) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.1.3 to 0.1.6 (#276) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocstrings-python from 1.7.4 to 1.7.5 (#279) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ipython from 8.17.2 to 8.18.1 (#280) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump pyarrow from 11.0.0 to 14.0.1 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocstrings-python from 1.7.3 to 1.7.4 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-material from 9.4.8 to 9.4.10 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mypy from 1.6.1 to 1.7.0 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump apache-airflow-providers-google [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump google-cloud-dataproc from 5.6.0 to 5.7.0 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump wandb from 0.13.11 to 0.16.0 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pytest-xdist from 3.3.1 to 3.4.0 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-material from 9.4.7 to 9.4.8 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump hail from 0.2.122 to 0.2.126 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump apache-airflow from 2.7.2 to 2.7.3 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump pre-commit from 2.21.0 to 3.5.0 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-minify-plugin from 0.5.0 to 0.7.1 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-material from 9.4.6 to 9.4.7 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump apache/airflow in /src/airflow [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump ruff from 0.0.287 to 0.1.3 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  add pydoclint to the project [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**deps-dev**\] bump apache-airflow-providers-google [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mkdocs-autolinks-plugin from 0.6.0 to 0.7.1 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps-dev**\] bump mypy from 0.971 to 1.6.1 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump pyspark from 3.3.0 to 3.3.3 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  lock hail version to 0.2.122 to fix #3088 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  lock hail version to 0.2.122 to fix #3088 [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**deps-dev**\] bump apache-airflow-providers-google [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  upgrade mkdocstrings-python from 0.7.1 to 1.7.3 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add scikit-learn [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `extensions.json` to vscode conf [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**session**\] assign driver and exec memory based on resources [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**ldindex**\] apply to all populations [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update configuration for new ldindex [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  downgrade project version to 0.1.4 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  downgrade from 3.10.9 to 3.10.8 for compatibility w/ dataproc [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update gitignore [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update mypy to 1.2.0 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add scikit-learn [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add wandb and xgboost to the project [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update python to 3.8.15 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update python to 3.8.15 [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  new target schema with the new `canonicalTranscript` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  pipeline run with most up to date gene index [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add pandas as a test dependency [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add pandas as a test dependency [\@Irene López](mailto:irene.lopezs@protonmail.com)

### 👷‍♂️ Ci


-  remove pyupgrade [\@Irene López](mailto:irene.lopezs@protonmail.com)

### 🚀 Chore


-  upgrade checkout (#346) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  delete makefile_deprecated (#329) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  review study locus and study index configs (#326) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  create code of conduct (#327) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  add `l2g_benchmark` notebook to compare with production results (#323) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


- \[**deps**\] bump actions/setup-python from 4 to 5 (#319) [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**airflow**\] schedule_interval deprecation warning (#293) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


- \[**l2ggoldstandard**\] add studyId to schema (#305) [\@Irene López](mailto:45119610+ireneisdoomed@users.noreply.github.com)


-  rename study_locus to credible_set for l2g [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove reference to confidence intervals [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove beta value interval calculation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  pre-commit autoupdate [\@pre-commit-ci[bot]](mailto:66853113+pre-commit-ci[bot]@users.noreply.github.com)


- \[**gold_standards**\] define gs labels as `L2GGoldStandard` attributes [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**overlaps**\] chromosome and statistics are not mandatory fields in the schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  change `sources` in gold standards schema to a nullable [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `variantId` to gold standards schema [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  make local SSDs a default [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  align default values with docstring [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove the num_local_ssds arg which has no effect [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  repartition data before processing [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  read input data from Google Storage [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  partition output data by chromosome [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  replace attributes with static methods for eQTL summary stats [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  replace attributes with static methods for eQTL study index [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add __init__.py for eQTL Catalogue [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  unify FinnGen config with eQTL Catalogue [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  add configuration [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  always error if the output data exists [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use more partitions for FinnGen [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use gs [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  use a higher RAM master machine [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  changes in config [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  delete local chain file [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**config**\] remove gnomad datasets from `gcp.yaml` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  do not set start_hail explicitly [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  merge main [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  unnecessary test [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  set default retries to 1 in dev setting [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  set the number of workers to 2 (min) [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove empty AIRFLOW_ID= from .env [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  leave AIRFLOW_UID empty by default [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update base Airflow version in Dockerfile [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove the __init__.py which is no longer needed [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


- \[**dag**\] add l2g and overlaps steps to `dag.yaml` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  move L2GFeature to datasets [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merge main [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove commented code [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merge main [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  extend docker image with `psycopg2-binary` for postgres set up [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  pin `apache-airflow-providers-google` version [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merge main [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  comment out failing airflow dag [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  add `pydoclint` to pre-commit [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**l2g**\] set to train by default [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  delete l2g_benchmark [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove unnecessary paths in gitignore [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename l2g config [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merge main [\@David Ochoa](mailto:dogcaesar@gmail.com)


- \[**env**\] update lock file [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**env**\] update lock file [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**feature**\] rename `feature_matrix.py` to `feature.py` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename workflow DAG following review [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  address review comments [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  rename DAG related modules [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update Python version to sync with the rest of the project [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  address review comments [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  make Dockerfile build more quiet [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove docker-compose - to be populated during installation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update Dockerfile [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  update requirements.txt [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  remove .env - to be populated during installation [\@Kirill Tsukanov](mailto:tsukanoffkirill@gmail.com)


-  fix typo for `gnomad_genomes` [\@Irene López](mailto:irene.lopezs@protonmail.com)


- \[**deps**\] bump actions/setup-python from 2 to 4 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump actions/cache from 2 to 3 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


- \[**deps**\] bump actions/checkout from 2 to 4 [\@dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com)


-  library updates in toml [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  fix conflicts with main [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  squash commit with main [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove exploration notebooks [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  comment line to compile config in makefile build [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rebase branch with main [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rebase branch with main [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  squash commit with main [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  rename function [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  duplicated logic [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merge main [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  unused dependency [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove unused function [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  align column names with main [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove print message [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  work in progress [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  deprecate `utils/configure` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove coverage xml in `test` rule [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merge conflicts [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  _get_schema renamed to get_schema [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  merge conflicts [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  hooks are now a main functionality of mkdocs [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  resolve conflicts with main branch [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  newline fix [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  restructuring the project [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  blacklist flake8 extension [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  dag renamed [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  minor comment [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  remove `my_ld_matrix.yaml` [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  update ld_index schema [\@Irene Lopez](mailto:irene.lopezs@protonmail.com)


-  remove unnecessary file [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  suggestions on structure (#99) [\@David Ochoa](mailto:ochoa@ebi.ac.uk)


-  logic cleaned up based on PR review [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  typing error [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  remove exploration notebooks [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  merge do-hydra branch [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  pull main branch [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  comment line to compile config in makefile build [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  wip gwas splitter [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  gitignore schemas markdown [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  cleanup deprecated [\@David Ochoa](mailto:dogcaesar@gmail.com)


-  update v2g schema to include distance features [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  delete commented lines [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  separate script for dependency installation (#47) [\@Kirill Tsukanov](mailto:tskir@users.noreply.github.com)


-  updated config and makefile [\@Irene López](mailto:irene.lopezs@protonmail.com)


-  adding function to calculate z-score [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)


-  exploring p-value conversion [\@Daniel Suveges](mailto:daniel.suveges@protonmail.com)
