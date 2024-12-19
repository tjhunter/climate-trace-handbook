# Operating procedures

Details the standard procedures on unix systems to run the project

## Running the notebooks

```fish
poetry shell
set -x PYTHONPATH $PWD:$PWD/src/ ; jupyter notebook
```

In a memory restricted environment:
```fish
set -x PYTHONPATH $PWD:$PWD/src/
systemd-run --scope -p MemoryMax=10G --user jupyter notebook
```

## Updating the book

```fish
set -x PYTHONPATH $PWD:$PWD/src/ ; jupyter-book build ct_book
```

or with limited memory:

```fish
systemd-run --scope -p MemoryMax=10G --user jupyter-book build ct_book
```

## Publish the book

```fish
cd ct_book
ghp-import -n -p -f _build/html
```

## Author new released and publish the package

Check version in pyproject.toml and `ctrace/__init__.py`
Only do it from main.

- update version in `pyproject.toml`
- update version in `src/trace/__init__.py`

```
poetry build
poetry publish
```