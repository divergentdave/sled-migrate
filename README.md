# sled-migrate

`sled-migrate` is a command-line tool to convert [sled](https://github.com/spacejam/sled) databases between incompatible alpha/beta version file formats.

## Example Usage

```bash
cargo run -- --inpath /my/original/database --inver 0.24 --outpath /my/converted/database --outver 0.28
```

## Help Text

```bash
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
        --inver <VERSION>     Input database version [possible values: 0.24, 0.25, 0.27, 0.28]
        --outpath <PATH>      Output database path
        --outver <VERSION>    Output database version [possible values: 0.24, 0.25, 0.27, 0.28]
```
