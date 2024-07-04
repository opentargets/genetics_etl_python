FROM ensemblorg/ensembl-vep:release_111.0

USER root

RUN apt-get update && apt-get -y install \
    wget \
    libncurses5-dev \
    libncursesw5-dev \
    libbz2-dev \
    liblzma-dev \
    sqlite3 \
    libsqlite3-dev \
    cpanminus \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN cpanm DBD::SQLite

RUN wget --progress=dot:giga https://github.com/samtools/samtools/releases/download/1.7/samtools-1.7.tar.bz2 && \
    tar xjvf samtools-1.7.tar.bz2 && \
    cd samtools-1.7 && \
    make && \
    make install

RUN wget --progress=dot:giga https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/loftee.sql.gz --directory-prefix=/opt/vep/ && \
    gunzip /opt/vep/loftee.sql.gz

# Make sure the mounting points exist:
RUN mkdir -p /mnt/disks/share/cache && \
    mkdir -p /mnt/disks/share/input && \
    mkdir -p /mnt/disks/share/output
