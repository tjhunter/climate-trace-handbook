
# The Unofficial Climate TRACE handbook

This reposity contains:
- the source to the Climate TRACE handbook for data scientists (published here)
- the ctrace library, used to access and manipulate the Climate TRACE dataset

**Relationship to the Climate TRACE project** Both projects are unofficial and unrelated
to the Climate TRACE project. The Climate TRACE consortium does not sponsor
or endorse any content in this repository. Any analysis, error, conclusion contained in these pages should not be attributed to the Climate TRACE project.

## Standard procedures

Using the fish terminal

```
poetry shell

set -x PYTHONPATH $PWD:$PWD/src/ ; jupyter notebook

set -x PYTHONPATH $PWD:$PWD/src/ ; jupyter-book build ct_book
```
https://github.com/scikit-learn/scikit-learn/blob/f07e0138b/sklearn/datasets/_openml.py#L753

https://github.com/awesomedata/awesome-public-datasets

https://alexgude.com/blog/where-to-host-public-datasets/
