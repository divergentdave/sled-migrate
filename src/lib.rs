use clap::{App, Arg};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
                .help("Input database path"),
        )
        .arg(
            Arg::with_name("inver")
                .long("inver")
                .takes_value(true)
                .value_name("VERSION")
                .required(true)
                .possible_values(&versions)
                .help("Input database version"),
        )
        .arg(
            Arg::with_name("outpath")
                .long("outpath")
                .takes_value(true)
                .value_name("PATH")
                .required(true)
                .help("Output database path"),
        )
        .arg(
            Arg::with_name("outver")
                .long("outver")
                .takes_value(true)
                .value_name("VERSION")
                .required(true)
                .possible_values(&versions)
                .help("Output database version"),
        )
        .get_matches();

    let in_path = matches.value_of_os("inpath").unwrap().into();
    let in_version = matches.value_of("inver").unwrap();
    let out_path = matches.value_of_os("outpath").unwrap().into();
    let out_version = matches.value_of("outver").unwrap();

    migrate(in_path, in_version, out_path, out_version);
}

fn migrate(in_path: PathBuf, in_version: &str, out_path: PathBuf, out_version: &str) {
    if !in_path.exists() {
        panic!("Input database {:?} does not exist", in_path);
    }
    if out_path.exists() {
        panic!("Output database {:?} already exists", out_path);
    }

    let in_adapter = open_dispatch(in_path, in_version);
    let out_adapter = open_dispatch(out_path, out_version);

    let in_checksum = in_adapter.checksum().unwrap();

    out_adapter.import(in_adapter.export());

    let out_checksum = out_adapter.checksum().unwrap();

    if in_checksum != out_checksum {
        panic!("Checksum of migrated database does not match, migration was unsuccessful!");
    }
}

fn open_dispatch(path: PathBuf, version: &str) -> Box<dyn SledAdapter> {
    match version {
        ver if ver == "0.24" => Box::new(Sled24::open(&path).unwrap()),
        ver if ver == "0.25" => Box::new(Sled25::open(&path).unwrap()),
        ver if ver == "0.27" => Box::new(Sled27::open(&path).unwrap()),
        ver if ver == "0.28" => Box::new(Sled28::open(&path).unwrap()),
        ver => panic!("Unsupported version {}", ver),
    }
}

type BoxedError = Box<dyn std::error::Error>;

trait TreeAdapter {
    fn iter(&self) -> BoxedTreeIter<'_>;
}

struct Tree24(Arc<sled_0_24::Tree>);

impl TreeAdapter for Tree24 {
    fn iter(&self) -> BoxedTreeIter<'_> {
        Box::new(self.0.iter().map(|kv_res| {
            let (k, v) = kv_res?;
            Ok((k, v.to_vec()))
        }))
    }
}

struct Tree25(Arc<sled_0_25::Tree>);

impl TreeAdapter for Tree25 {
    fn iter(&self) -> BoxedTreeIter<'_> {
        Box::new(self.0.iter().map(|kv_res| {
            let (k, v) = kv_res?;
            Ok((k.to_vec(), v.to_vec()))
        }))
    }
}

struct Tree27(Arc<sled_0_27::Tree>);

impl TreeAdapter for Tree27 {
    fn iter(&self) -> BoxedTreeIter<'_> {
        Box::new(self.0.iter().map(|kv_res| {
            let (k, v) = kv_res?;
            Ok((k.to_vec(), v.to_vec()))
        }))
    }
}

struct Tree28(sled_0_28::Tree);

impl TreeAdapter for Tree28 {
    fn iter(&self) -> BoxedTreeIter<'_> {
        Box::new(self.0.iter().map(|kv_res| {
            let (k, v) = kv_res?;
            Ok((k.to_vec(), v.to_vec()))
        }))
    }
}

type BoxedTreeIter<'a> = Box<(dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), BoxedError>> + 'a)>;

type BoxedKeyValIter = Box<dyn Iterator<Item = Vec<Vec<u8>>>>;

trait SledAdapter {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>;
    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>);
    fn tree_names(&self) -> Vec<Vec<u8>>;
    fn open_tree(&self, name: &[u8]) -> Result<Box<dyn TreeAdapter>, BoxedError>;
    fn checksum(&self) -> Result<u32, BoxedError>;
}

struct Sled24(sled_0_24::Db);

impl Sled24 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, BoxedError> {
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
            let kvs: Vec<Vec<Vec<u8>>> = tree
                .iter()
                .map(|kv| {
                    let kv = kv.unwrap();
                    vec![kv.0.to_vec(), kv.1.to_vec()]
                })
                .collect();
            let boxed_iter: BoxedKeyValIter = Box::new(kvs.into_iter());
            ret.push((b"tree".to_vec(), name, boxed_iter));
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

    fn tree_names(&self) -> Vec<Vec<u8>> {
        self.0.tree_names()
    }

    fn open_tree(&self, name: &[u8]) -> Result<Box<dyn TreeAdapter>, BoxedError> {
        Ok(Box::new(Tree24(self.0.open_tree(name)?)))
    }

    fn checksum(&self) -> Result<u32, BoxedError> {
        checksum_polyfill(self)
    }
}

struct Sled25(sled_0_25::Db);

impl Sled25 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, BoxedError> {
        Ok(Self(sled_0_25::Db::open(path)?))
    }
}

impl SledAdapter for Sled25 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        fn mapfn<I: 'static + Iterator<Item = Vec<Vec<u8>>>>(
            (collection_type, collection_name, iter): (Vec<u8>, Vec<u8>, I),
        ) -> (Vec<u8>, Vec<u8>, BoxedKeyValIter) {
            (collection_type, collection_name, Box::new(iter))
        }

        self.0.export().into_iter().map(mapfn).collect()
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        self.0.import(export)
    }

    fn tree_names(&self) -> Vec<Vec<u8>> {
        self.0.tree_names()
    }

    fn open_tree(&self, name: &[u8]) -> Result<Box<dyn TreeAdapter>, BoxedError> {
        Ok(Box::new(Tree25(self.0.open_tree(name)?)))
    }

    fn checksum(&self) -> Result<u32, BoxedError> {
        checksum_polyfill(self)
    }
}

struct Sled27(sled_0_27::Db);

impl Sled27 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, BoxedError> {
        Ok(Self(sled_0_27::Db::open(path)?))
    }
}

impl SledAdapter for Sled27 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        fn mapfn<I: 'static + Iterator<Item = Vec<Vec<u8>>>>(
            (collection_type, collection_name, iter): (Vec<u8>, Vec<u8>, I),
        ) -> (Vec<u8>, Vec<u8>, BoxedKeyValIter) {
            (collection_type, collection_name, Box::new(iter))
        }

        self.0.export().into_iter().map(mapfn).collect()
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        self.0.import(export)
    }

    fn tree_names(&self) -> Vec<Vec<u8>> {
        self.0.tree_names()
    }

    fn open_tree(&self, name: &[u8]) -> Result<Box<dyn TreeAdapter>, BoxedError> {
        Ok(Box::new(Tree27(self.0.open_tree(name)?)))
    }

    fn checksum(&self) -> Result<u32, BoxedError> {
        checksum_polyfill(self)
    }
}

struct Sled28(sled_0_28::Db);

impl Sled28 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, BoxedError> {
        Ok(Self(sled_0_28::Db::open(path)?))
    }
}

impl SledAdapter for Sled28 {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)> {
        fn mapfn<I: 'static + Iterator<Item = Vec<Vec<u8>>>>(
            (collection_type, collection_name, iter): (Vec<u8>, Vec<u8>, I),
        ) -> (Vec<u8>, Vec<u8>, BoxedKeyValIter) {
            (collection_type, collection_name, Box::new(iter))
        }

        self.0.export().into_iter().map(mapfn).collect()
    }

    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>) {
        self.0.import(export)
    }

    fn tree_names(&self) -> Vec<Vec<u8>> {
        self.0.tree_names()
    }

    fn open_tree(&self, name: &[u8]) -> Result<Box<dyn TreeAdapter>, BoxedError> {
        Ok(Box::new(Tree28(self.0.open_tree(name)?)))
    }

    fn checksum(&self) -> Result<u32, BoxedError> {
        checksum_polyfill(self)
    }
}

fn checksum_polyfill(adapter: &dyn SledAdapter) -> Result<u32, BoxedError> {
    // Checksum was released after 0.28.0, so we replicate its function for it and earlier versions
    let mut tree_names = adapter.tree_names();
    tree_names.sort_unstable();

    let mut hasher = crc32fast::Hasher::new();

    for name in tree_names.into_iter() {
        hasher.update(&name);

        let tree = adapter.open_tree(&name)?;
        for kv_res in tree.iter() {
            let (k, v) = kv_res?;
            hasher.update(&k);
            hasher.update(&v);
        }
    }

    Ok(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::migrate;
    use std::fs::remove_dir_all;
    use std::path::PathBuf;

    macro_rules! fill {
        ($db:ident) => {
            fill!($db, insert, remove)
        };
        ($db:ident, $insert:ident, $remove:ident) => {
            $db.$insert(b"key", b"value").unwrap();
            $db.$insert(b"removed", b"todo").unwrap();
            $db.$remove(b"removed").unwrap();
            $db.$insert(b"foo", b"bar").unwrap();
            let tree = $db.open_tree(b"alternate tree").unwrap();
            tree.$insert(b"different data", b"AAAA").unwrap();
            drop(tree);
        };
    }

    macro_rules! check {
        ($db:ident) => {
            assert_eq!(
                $db.tree_names(),
                vec![b"__sled__default".to_vec(), b"alternate tree".to_vec()]
            );

            let mut iter = $db
                .iter()
                .map(Result::unwrap)
                .map(|(k, v)| (k.to_vec(), v.to_vec()));
            assert_eq!(iter.next(), Some((b"foo".to_vec(), b"bar".to_vec())));
            assert_eq!(iter.next(), Some((b"key".to_vec(), b"value".to_vec())));
            assert_eq!(iter.next(), None);

            let tree = $db.open_tree(b"alternate tree").unwrap();
            let mut iter = tree
                .iter()
                .map(Result::unwrap)
                .map(|(k, v)| (k.to_vec(), v.to_vec()));
            assert_eq!(
                iter.next(),
                Some((b"different data".to_vec(), b"AAAA".to_vec()))
            );
            assert_eq!(iter.next(), None);
        };
    }

    #[test]
    fn migrate_24_25() {
        let from_dir = PathBuf::from("db2425a");
        let to_dir = PathBuf::from("db2425b");

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_24::Db::start_default(&from_dir).unwrap();
        fill!(db, set, del);
        drop(db);

        migrate(from_dir, "0.24", to_dir.clone(), "0.25");

        let db = sled_0_25::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn migrate_25_27() {
        let from_dir = PathBuf::from("db2527a");
        let to_dir = PathBuf::from("db2527b");

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_25::Db::open(&from_dir).unwrap();
        fill!(db);
        drop(db);

        migrate(from_dir, "0.25", to_dir.clone(), "0.27");

        let db = sled_0_27::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn migrate_27_28() {
        let from_dir = PathBuf::from("db2728a");
        let to_dir = PathBuf::from("db2728b");

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_27::Db::open(&from_dir).unwrap();
        fill!(db);
        drop(db);

        migrate(from_dir, "0.27", to_dir.clone(), "0.28");

        let db = sled_0_28::Db::open(to_dir).unwrap();
        check!(db);
    }
}
