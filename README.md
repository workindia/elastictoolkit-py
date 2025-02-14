# elastictoolkit-py

This package provides base classes and helpers for simplifying Elasticsearch operations, including indexing and query building

## Features
* TODO

## Installation
```
pip install -U <package-dist>
```

## Build Requirements
- Make
- Python

## Initialise Build Environment
One time initialisation
```
make init
```

## Make a release build
1. Ensure that working directory is clean and all files are commited.
2. Bump the version. Please read [Semantic Versioning 2.0.0](http://semver.org/) before bumping versions.
   ```
   make release PART=[major/minor/patch]
   ```
3. Update the CHANGELOG.md.
4. Push release tags.
   ```
   git push origin --tags
   ```
5. Make a release build.
   ```
   make dist
   ```

## Publish your changes
```
make deploy TYPE=[prod/stag/dev]
```

## Changelog

Please find the changelog here: [CHANGELOG.md](CHANGELOG.md)

## Authors

elastictoolkit-py was written by [Nikhil Kumar](mailto:nikhil.kumar@workindia.in).