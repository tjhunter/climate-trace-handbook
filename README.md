
# The Unofficial Climate TRACE handbook

This reposity contains:
- the source to the Climate TRACE handbook for data scientists (published [here](https://tjhunter.github.io/climate-trace-handbook/))
- the `ctrace` library, used to access and manipulate the Climate TRACE dataset

**Relationship to the Climate TRACE project** Both projects are unofficial and unrelated
to the Climate TRACE project. The Climate TRACE consortium does not sponsor
or endorse any content in this repository. Any analysis, error, conclusion contained in these pages should not be attributed to the Climate TRACE project.

## Standard procedures

Using the fish terminal

```
poetry shell

set -x PYTHONPATH $PWD:$PWD/src/ ; jupyter notebook

set -x PYTHONPATH $PWD:$PWD/src/ ; jupyter-book build ct_book

ghp-import -n -p -f _build/html
```
