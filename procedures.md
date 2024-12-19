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

## Publish the book

```fish
ghp-import -n -p -f _build/html
```

## Publish the package

Only do it from main.

```
poetry publish
```