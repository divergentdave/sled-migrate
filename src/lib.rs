use clap::{App, Arg};
use std::path::{Path, PathBuf};

pub fn main() {
    let versions = ["0.24", "0.25", "0.27", "0.28"];
    let matches = App::new("sled-migrate")
        .version("0.1.0")
        .author("David Cook <divergentdave@gmail.com>")
        .about(
            "A small wrapper to migrate sled databases between \
             file format-incompatible alpha and beta versions.",
        )
        .arg(
            Arg::with_name("inpath")
                .long("inpath")
                .takes_value(true)
                .value_name("PATH")
                .required(true)
                .help("Input database path")
        )
        .arg(
            Arg::with_name("inver")
                .long("inver")
                .takes_value(true)
                .value_name("VERSION")
                .required(true)
                .possible_values(&versions)
                .help("Input database version")
        )
        .arg(
            Arg::with_name("outpath")
                .long("outpath")
                .takes_value(true)
                .value_name("PATH")
                .required(true)
                .help("Output database path")
        )
        .arg(
            Arg::with_name("outver")
                .long("outver")
                .takes_value(true)
                .value_name("VERSION")
                .required(true)
                .possible_values(&versions)
                .help("Output database version")
        )
        .get_matches();

    let in_path: PathBuf = matches.value_of_os("inpath").unwrap().into();
    let in_version = matches.value_of("inver").unwrap();
    let out_path: PathBuf = matches.value_of_os("outpath").unwrap().into();
    let out_version = matches.value_of("outver").unwrap();

    if !in_path.exists() {
        panic!("Input database {:?} does not exist", in_path);
    }
    if out_path.exists() {
        panic!("Output database {:?} already exists", out_path);
    }

    let in_adapter = open_dispatch(in_path, in_version);
    let out_adapter = open_dispatch(out_path, out_version);

    out_adapter.import(in_adapter.export());
}

fn open_dispatch(path: PathBuf, version: &str) -> Box<dyn SledAdapter> {
    match version {
        ver if ver == "0.24" => {
            Box::new(Sled24::open(&path).unwrap())
        }
        ver if ver == "0.25" => {
            Box::new(Sled25::open(&path).unwrap())
        }
        ver if ver == "0.27" => {
            Box::new(Sled27::open(&path).unwrap())
        }
        ver if ver == "0.28" => {
            Box::new(Sled28::open(&path).unwrap())
        }
        ver => panic!("Unsupported version {}", ver),
    }
}

type BoxedKeyValIter = Box<dyn Iterator<Item = Vec<Vec<u8>>>>;

trait SledAdapter {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>;
    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>);
    // fn checksum(&self) -> Result<u32>;
}

struct Sled24 (sled_0_24::Db);

impl Sled24 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self(sled_0_24::Db::start_default(path)?))
    }
}

impl SledAdapter for Sled24 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        // Export had not yet been implemented, so we replicate its function here.
        let mut ret = vec![];

        for name in self.0.tree_names().into_iter() {
            let tree = self.0.open_tree(&name).unwrap();
            // Note that sled::Iter has a lifetime bounded by the Tree it came from,
            // so we have to .collect() it.
            let kvs: Vec<Vec<Vec<u8>>> = tree.iter().map(|kv| {
                let kv = kv.unwrap();
                vec![kv.0.to_vec(), kv.1.to_vec()]
            }).collect();
            let boxed_iter: BoxedKeyValIter = Box::new(kvs.into_iter());
            ret.push((
                b"tree".to_vec(),
                name,
                boxed_iter,
            ));
        }

        ret
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        // Import had not yet been implemented, so we replicate its function here.
        for (collection_type, collection_name, collection_boxed_iter) in export {
            match collection_type {
                ref t if t == b"tree" => {
                    let tree = self.0.open_tree(collection_name).unwrap();
                    for mut kv in collection_boxed_iter {
                        let v = kv.pop().unwrap();
                        let k = kv.pop().unwrap();
                        tree.set(k, v).unwrap();
                    }
                }
                other => panic!("unknown collection type {:?}", other),
            }
        }
    }
}

struct Sled25 (sled_0_25::Db);

impl Sled25 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self(sled_0_25::Db::open(path)?))
    }
}

impl SledAdapter for Sled25 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        fn mapfn<I: 'static + Iterator<Item = Vec<Vec<u8>>>>((collection_type, collection_name, iter): (Vec<u8>, Vec<u8>, I)) -> (Vec<u8>, Vec<u8>, BoxedKeyValIter) {
            (collection_type, collection_name, Box::new(iter))
        }

        self.0.export().into_iter().map(mapfn).collect()
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        self.0.import(export)
    }
}

struct Sled27 (sled_0_27::Db);

impl Sled27 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self(sled_0_27::Db::open(path)?))
    }
}

impl SledAdapter for Sled27 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        fn mapfn<I: 'static + Iterator<Item = Vec<Vec<u8>>>>((collection_type, collection_name, iter): (Vec<u8>, Vec<u8>, I)) -> (Vec<u8>, Vec<u8>, BoxedKeyValIter) {
            (collection_type, collection_name, Box::new(iter))
        }

        self.0.export().into_iter().map(mapfn).collect()
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        self.0.import(export)
    }
}

struct Sled28 (sled_0_28::Db);

impl Sled28 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self(sled_0_28::Db::open(path)?))
    }
}

impl SledAdapter for Sled28 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        fn mapfn<I: 'static + Iterator<Item = Vec<Vec<u8>>>>((collection_type, collection_name, iter): (Vec<u8>, Vec<u8>, I)) -> (Vec<u8>, Vec<u8>, BoxedKeyValIter) {
            (collection_type, collection_name, Box::new(iter))
        }

        self.0.export().into_iter().map(mapfn).collect()
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        self.0.import(export)
    }
}
