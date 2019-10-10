# sled-migrate

`sled-migrate` is a command-line tool to convert [sled](https://github.com/spacejam/sled) databases between incompatible alpha/beta version file formats.

## Example Usage

```bash
cargo run -- --inpath /my/original/database --inver 0.24 --outpath /my/converted/database --outver 0.28
```

## Help Text

```
sled-migrate 0.1.0
David Cook <divergentdave@gmail.com>
A small wrapper to migrate sled databases between file format-incompatible alpha and beta versions.

USAGE:
    sled-migrate --inpath <PATH> --inver <VERSION> --outpath <PATH> --outver <VERSION>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --inpath <PATH>       Input database path
        --inver <VERSION>     Input database version [possible values: 0.23, 0.24, 0.25, 0.28]
        --outpath <PATH>      Output database path
        --outver <VERSION>    Output database version [possible values: 0.23, 0.24, 0.25, 0.28]
```

## Versions

From sled version 0.25 through sled version 0.28, file format compatibility was determined by the minor version number of the pagecache crate. Subsequently, the pagecache crate was merged into the sled crate, and now file format compatibility is determined by the minor version number of the sled crate. `sled-migrate` uses the most recent release of each file format epoch.

| sled version(s) | pagecache version | `--inver` / `--outver` choice |
| --- | --- | --- |
| (unreleased) | N/A | TBD |
| 0.28 (0.27 and 0.26) | 0.19 | `0.28` |
| 0.25 | 0.18 | `0.25` |
| 0.24 | 0.17 | `0.24` |
| 0.23 | 0.16 | `0.23` |
