
# The Unofficial Climate TRACE handbook

This reposity contains:
- the source to the Climate TRACE handbook for data scientists (published [here](https://tjhunter.github.io/climate-trace-handbook/))
- the [ctrace](https://pypi.org/project/climate-trace/) python package, used to access and manipulate the Climate TRACE dataset

**Relationship to the Climate TRACE project** Both projects are unofficial and unrelated
to the Climate TRACE project. The Climate TRACE consortium does not sponsor
or endorse any content in this repository. Any analysis, error, conclusion contained in these pages should not be attributed to the Climate TRACE project.

## Changelog

### 0.4

- New data release, which solves a number of inconsistencies in the data.
- Added CH4, N2O
- Improvements to the ingestion pipeline.
- Polars version has also been upgraded to benefit from the latest Improvements in stability.

From this release, `ctrace` will take monthly snapshots of the Climate TRACE dataset. This
dataset is currently not versioned and does not publish checksums, which limits the interest
of pinning to specific cheksums.
