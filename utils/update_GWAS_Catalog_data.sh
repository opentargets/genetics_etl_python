#!/usr/bin/env bash

# Function to get the most recent date:
get_most_recent(){
    cat $1 | perl -lane 'push @a, $_ if $_ =~ /^\d+$/; END {@a = sort { $a <=> $b} @a; print pop @a }'
}

# Function to return the path the to the most recent release:
get_release_url(){
    YEAR=$(curl -s --list-only ${BASE_URL}/releases/ | get_most_recent)
    MONTH=$(curl -s --list-only ${BASE_URL}/releases/${YEAR}/  | get_most_recent)
    DAY=$(curl -s --list-only ${BASE_URL}/releases/${YEAR}/${MONTH}/  | get_most_recent)
    echo $YEAR $MONTH $DAY
}

# Function to get the Ensembl and EFO version which used to ground GWAS data:
get_release_info(){
    curl -s "${1}" | jq -r '"\(.ensemblbuild) \(.efoversion)"'
}

logging(){
    log_prompt="[$(date "+%Y.%m.%d %H:%M")]"
    echo "${log_prompt} $@" >> ${LOG_FILE}
}

upload_file_to_gcp(){
    FILENAME=${1}
    TARGET=${2}
    # Test if file exists:
    if [ ! -f ${FILENAME} ]; then
        logging "File ${FILENAME} does not exist."
        return
    fi

    logging "Copying ${FILENAME} to GCP..."
    gsutil -mq cp file://$(pwd)/${FILENAME} ${TARGET}

    # Test if file was successfully uploaded:
    if [ $? -ne 0 ]; then
        logging "File ${FILENAME} failed to upload."
    fi
}

fetch_from_ftp(){
    URL=${1}
    TARGET=${2}
    wget -q ${URL} -O ${TARGET}
    if [ $? -ne 0 ]; then
        logging "Failed to fetch ${URL}"
        return
    else
        logging "File ${TARGET} saved."
    fi
}

# Resources:
export BASE_URL=ftp://ftp.ebi.ac.uk/pub/databases/gwas
export RELEASE_INFO_URL=https://www.ebi.ac.uk/gwas/api/search/stats
export GCP_TARGET=gs://gwas_catalog_data
export LOG_FILE=gwas_catalog_data_update.log

export GWAS_CATALOG_STUDY_CURATION_URL=https://raw.githubusercontent.com/opentargets/curation/master/genetics/GWAS_Catalog_study_curation.tsv

ASSOCIATION_FILE=gwas_catalog_associations_ontology_annotated.tsv
PUBLISHED_STUDIES_FILE=gwas_catalog_download_studies.tsv
PUBLISHED_ANCESTRIES_FILE=gwas_catalog_download_ancestries.tsv
UNPUBLISHED_STUDIES_FILE=gwas_catalog_unpublished_studies.tsv
UNPUBLISHED_ANCESTRIES_FILE=gwas_catalog_unpublished_ancestries.tsv
HARMONISED_LIST_FILE=harmonised_list.txt
GWAS_CATALOG_STUDY_CURATION_FILE=gwas_catalog_study_curation.tsv

# Remove log file if exists:
if [ -f ${LOG_FILE} ]; then
    rm -rf ${LOG_FILE}
fi

logging "Extracing data from: ${BASE_URL}"
logging "Release info fetched fom: ${RELEASE_INFO_URL}"
logging "Resulting files uploaded to: ${GCP_TARGET}"

# Capturing release date:
read YEAR MONTH DAY < <(get_release_url)
logging "Most recent GWAS Catalog release: ${YEAR}/${MONTH}/${DAY}"

# Capturing release metadata:
read ENSEMBL EFO < <(get_release_info ${RELEASE_INFO_URL})
logging "Genes were mapped to v${ENSEMBL} Ensembl release."
logging "Diseases were mapped to ${EFO} EFO release."

# Constructing FTP URL to access the most recent release:
RELEASE_URL=${BASE_URL}/releases/${YEAR}/${MONTH}/${DAY}
logging "Datafiles are fetching from ${RELEASE_URL}"

Fetching files while assigning properly dated and annotated names:
fetch_from_ftp ${RELEASE_URL}/gwas-catalog-associations_ontology-annotated.tsv ${ASSOCIATION_FILE}

fetch_from_ftp ${RELEASE_URL}/gwas-catalog-download-studies-v1.0.3.1.txt ${PUBLISHED_STUDIES_FILE}

fetch_from_ftp ${RELEASE_URL}/gwas-catalog-unpublished-studies-v1.0.3.1.tsv ${UNPUBLISHED_STUDIES_FILE}

fetch_from_ftp ${RELEASE_URL}/gwas-catalog-download-ancestries-v1.0.3.1.txt ${PUBLISHED_ANCESTRIES_FILE}

fetch_from_ftp ${RELEASE_URL}/gwas-catalog-unpublished-ancestries-v1.0.3.1.tsv ${UNPUBLISHED_ANCESTRIES_FILE}

fetch_from_ftp ${BASE_URL}/summary_statistics/harmonised_list.txt ${HARMONISED_LIST_FILE}

fetch_from_ftp ${GWAS_CATALOG_STUDY_CURATION_URL} ${GWAS_CATALOG_STUDY_CURATION_FILE}

logging "Copying files to GCP..."

upload_file_to_gcp ${ASSOCIATION_FILE} ${GCP_TARGET}/curated_inputs/
upload_file_to_gcp ${PUBLISHED_STUDIES_FILE} ${GCP_TARGET}/curated_inputs/
upload_file_to_gcp ${PUBLISHED_ANCESTRIES_FILE} ${GCP_TARGET}/curated_inputs/
upload_file_to_gcp ${HARMONISED_LIST_FILE} ${GCP_TARGET}/curated_inputs/
upload_file_to_gcp ${UNPUBLISHED_STUDIES_FILE} ${GCP_TARGET}/curated_inputs/
upload_file_to_gcp ${UNPUBLISHED_ANCESTRIES_FILE} ${GCP_TARGET}/curated_inputs/
upload_file_to_gcp ${GWAS_CATALOG_STUDY_CURATION_FILE} ${GCP_TARGET}/manifests/


logging "Files successfully uploaded."
logging "Removing local files..."
rm ${ASSOCIATION_FILE} \
    ${PUBLISHED_STUDIES_FILE} \
    ${PUBLISHED_ANCESTRIES_FILE} \
    ${HARMONISED_LIST_FILE} \
    ${UNPUBLISHED_STUDIES_FILE} \
    ${UNPUBLISHED_ANCESTRIES_FILE} \
    ${GWAS_CATALOG_STUDY_CURATION_FILE}

# Uploading log file to GCP manifest folder:
logging "Uploading log file to GCP manifest folder..."
upload_file_to_gcp ${LOG_FILE} ${GCP_TARGET}/manifests/
cat $LOG_FILE
