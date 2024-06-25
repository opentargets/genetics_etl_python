---
title: Contributing guidelines
---

# Contributing guidelines

## One-time configuration

The steps in this section only ever need to be done once on any particular system.

For Google Cloud configuration:

1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install.

1. Log in to your work Google Account: run `gcloud auth login` and follow instructions.

1. Obtain Google application credentials: run `gcloud auth application-default login` and follow instructions.

Check that you have the `make` utility installed, and if not (which is unlikely), install it using your system package manager.

Check that you have `java` installed.

## Environment configuration

Run `make setup-dev` to install/update the necessary packages and activate the development environment. You need to do this every time you open a new shell.

It is recommended to use VS Code as an IDE for development.

## How to run the code

All pipelines in this repository are intended to be run in Google Dataproc. Running them locally is not currently supported.

In order to run the code:

1. Manually edit your local `src/airflow/dags/*` file and comment out the steps you do not want to run.

2. Manually edit your local `pyproject.toml` file and modify the version of the code.

   - This must be different from the version used by any other people working on the repository to avoid any deployment conflicts, so it's a good idea to use your name, for example: `1.2.3+jdoe`.
   - You can also add a brief branch description, for example: `1.2.3+jdoe.myfeature`.
   - Note that the version must comply with [PEP440 conventions](https://peps.python.org/pep-0440/#normalization), otherwise Poetry will not allow it to be deployed.
   - Do not use underscores or hyphens in your version name. When building the WHL file, they will be automatically converted to dots, which means the file name will no longer match the version and the build will fail. Use dots instead.

3. Manually edit your local `src/airflow/dags/common_airflow.py` and set `GENTROPY_VERSION` to the same version as you did in the previous step.

4. Run `make build`.

   - This will create a bundle containing the neccessary code, configuration and dependencies to run the ETL pipeline, and then upload this bundle to Google Cloud.
   - A version specific subpath is used, so uploading the code will not affect any branches but your own.
   - If there was already a code bundle uploaded with the same version number, it will be replaced.

5. Open Airflow UI and run the DAG.

## Contributing checklist

When making changes, and especially when implementing a new module or feature, it's essential to ensure that all relevant sections of the code base are modified.

- [ ] Run `make check`. This will run the linter and formatter to ensure that the code is compliant with the project conventions.
- [ ] Develop unit tests for your code and run `make test`. This will run all unit tests in the repository, including the examples appended in the docstrings of some methods.
- [ ] Update the configuration if necessary.
- [ ] Update the documentation and check it with `make build-documentation`. This will start a local server to browse it (URL will be printed, usually `http://127.0.0.1:8000/`)

For more details on each of these steps, see the sections below.

### Documentation

- If during development you had a question which wasn't covered in the documentation, and someone explained it to you, add it to the documentation. The same applies if you encountered any instructions in the documentation which were obsolete or incorrect.
- Documentation autogeneration expressions start with `:::`. They will automatically generate sections of the documentation based on class and method docstrings. Be sure to update them for:
  - Datasource main page, for example: `docs/python_api/datasources/finngen/_finngen.md`
  - Dataset definitions, for example: `docs/python_api/datasources/finngen/study_index.md`
  - Step definition, for example: `docs/python_api/steps/finngen_sumstat_preprocess.md`

### Configuration

- Input and output paths in `config/datasets/ot_gcp.yaml`
- Step configuration, for example: `config/step/ot_finngen_sumstat_preprocess.yaml`

### Classes

- Datasource init, for example: `src/gentropy/datasource/finngen/__init__.py`
- Dataset classes, for example: `src/gentropy/datasource/finngen/study_index.py` → `FinnGenStudyIndex`
- Step main running class, for example: `src/gentropy/finngen_sumstat_preprocess.py`

### Tests

- Test study fixture in `tests/conftest.py`, for example: `mock_study_index_finngen` in that module
- Test sample data, for example: `tests/gentropy/data_samples/finngen_studies_sample.json`
- Test definition, for example: `tests/dataset/test_study_index.py` → `test_study_index_finngen_creation`)

### Orchestration

- Airflow DAG, for example: `src/airflow/dags/finngen_harmonisation.py`
