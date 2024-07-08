---
title: Clumping
---

# Clumping

Clumping is a commonly used post-processing method that allows for the identification of independent association signals from GWAS summary statistics and curated associations. This process is critical because of the complex linkage disequilibrium (LD) structure in human populations, which can result in multiple statistically significant associations within the same genomic region. Clumping methods help reduce redundancy in GWAS results and ensure that each reported association represents an independent signal.

We have implemented two clumping methods:

1. **Distance-based clumping:** Uses genomic window to clump the significant SNPs into one hit.
2. **LD-based clumping:** Uses genomic window and LD to clump the significant SNPs into one hit.
3. **Locus-breaker clumping:** Applies a distance cutoff between baseline significant SNPs. Returns the start and end position of the locus as well.

The algorithmic logic is similar to classic clumping approaches from PLINK (Reference: [PLINK Clump Documentation](https://zzz.bwh.harvard.edu/plink/clump.shtml)). See details below:

# Distance-based clumping

::: gentropy.method.window_based_clumping.WindowBasedClumping

# LD-based clumping:

::: gentropy.method.clump.LDclumping

# Locus-breaker clumping

::: gentropy.method.locus_breaker_clumping.LocusBreakerClumping
