use clap::{App, Arg};
use fs2::FileExt;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn main<I: Iterator<Item = String>>(args: I) {
    let versions: Vec<&'static str> = SledVersion::LIST.iter().map(SledVersion::as_text).collect();
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
        .get_matches_from(args);

    let in_path: PathBuf = matches.value_of_os("inpath").unwrap().into();
    let out_path: PathBuf = matches.value_of_os("outpath").unwrap().into();

    let in_version = match matches.value_of("inver") {
        Some(version) => match SledVersion::from_text(version) {
            Some(version) => version,
            None => panic!("Unsupporeted version {}", version),
        }
        None => match version_detect(&in_path) {
            Ok(Some(version)) => version,
            Ok(None) => panic!("Couldn't detect the input database's version. It is likely 0.24 or earlier. Specify its version with the \"--inver\" option."),
            Err(err) => panic!("Error while reading input database {}", err),
        }
    };
    let out_version = match matches.value_of("outver") {
        Some(version) => match SledVersion::from_text(version) {
            Some(version) => version,
            None => panic!("Unsupporeted version {}", version),
        },
        None => panic!("Output database version is required"),
    };

    migrate(&in_path, in_version, &out_path, out_version);
}

fn migrate(in_path: &Path, in_version: SledVersion, out_path: &Path, out_version: SledVersion) {
    if !in_path.exists() {
        panic!("Input database {:?} does not exist", in_path);
    }
    if out_path.exists() {
        panic!("Output database {:?} already exists", out_path);
    }

    let in_adapter = open_dispatch(&in_path, in_version);
    let out_adapter = open_dispatch(&out_path, out_version);

    let in_checksum = in_adapter
        .checksum()
        .expect("Couldn't calculate checksum of input database");

    out_adapter.import(in_adapter.export());

    drop(out_adapter);
    block_file_unlocked(&out_path);
    let out_adapter = open_dispatch(&out_path, out_version);

    let out_checksum = out_adapter
        .checksum()
        .expect("Couldn't calculate checksum of output database");

    if in_checksum != out_checksum {
        panic!("Checksum of migrated database does not match, migration was unsuccessful!");
    }
}

fn block_file_unlocked(path: &Path) {
    let path = path.join("db");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.lock_exclusive().unwrap();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SledVersion {
    Sled23,
    Sled24,
    Sled25,
    Sled28,
    Sled29,
}

impl SledVersion {
    const LIST: [Self; 5] = [
        Self::Sled23,
        Self::Sled24,
        Self::Sled25,
        Self::Sled28,
        Self::Sled29,
    ];

    fn as_text(&self) -> &'static str {
        match self {
            Self::Sled23 => "0.23",
            Self::Sled24 => "0.24",
            Self::Sled25 => "0.25",
            Self::Sled28 => "0.28",
            Self::Sled29 => "0.29",
        }
    }

    fn from_text(text: &str) -> Option<Self> {
        match text {
            "0.23" => Some(Self::Sled23),
            "0.24" => Some(Self::Sled24),
            "0.25" => Some(Self::Sled25),
            "0.28" => Some(Self::Sled28),
            "0.29" => Some(Self::Sled29),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum Error {
    Io(io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::Io(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

fn open_dispatch(path: &Path, version: SledVersion) -> Box<dyn SledAdapter> {
    match version {
        SledVersion::Sled23 => Box::new(Sled23::open(&path).expect("Couldn't open database")),
        SledVersion::Sled24 => Box::new(Sled24::open(&path).expect("Couldn't open database")),
        SledVersion::Sled25 => Box::new(Sled25::open(&path).expect("Couldn't open database")),
        SledVersion::Sled28 => Box::new(Sled28::open(&path).expect("Couldn't open database")),
        SledVersion::Sled29 => Box::new(Sled29::open(&path).expect("Couldn't open database")),
    }
}

type BoxedError = Box<dyn std::error::Error>;

trait TreeAdapter {
    fn iter(&self) -> BoxedTreeIter<'_>;
}

macro_rules! old_tree_adapter {
    ($tree:ident) => {
        impl TreeAdapter for $tree {
            fn iter(&self) -> BoxedTreeIter<'_> {
                Box::new(self.0.iter().map(|kv_res| {
                    let (k, v) = kv_res?;
                    Ok((k, v.to_vec()))
                }))
            }
        }
    };
}

macro_rules! new_tree_adapter {
    ($tree:ident) => {
        impl TreeAdapter for $tree {
            fn iter(&self) -> BoxedTreeIter<'_> {
                Box::new(self.0.iter().map(|kv_res| {
                    let (k, v) = kv_res?;
                    Ok((k.to_vec(), v.to_vec()))
                }))
            }
        }
    };
}

struct Tree23(Arc<sled_0_23::Tree>);

old_tree_adapter!(Tree23);

struct Tree24(Arc<sled_0_24::Tree>);

old_tree_adapter!(Tree24);

struct Tree25(Arc<sled_0_25::Tree>);

new_tree_adapter!(Tree25);

struct Tree28(sled_0_28::Tree);

new_tree_adapter!(Tree28);

struct Tree29(sled_0_29::Tree);

new_tree_adapter!(Tree29);

type BoxedTreeIter<'a> = Box<(dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), BoxedError>> + 'a)>;

type BoxedKeyValIter = Box<dyn Iterator<Item = Vec<Vec<u8>>>>;

trait SledAdapter {
    fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>;
    fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, BoxedKeyValIter)>);
    fn tree_names(&self) -> Vec<Vec<u8>>;
    fn open_tree(&self, name: &[u8]) -> Result<Box<dyn TreeAdapter>, BoxedError>;
    fn checksum(&self) -> Result<u32, BoxedError>;
}

struct Sled23(sled_0_23::Db);

impl Sled23 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, BoxedError> {
        Ok(Self(sled_0_23::Db::start_default(path)?))
    }
}

impl SledAdapter for Sled23 {
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
        Ok(Box::new(Tree23(self.0.open_tree(name)?)))
    }

    fn checksum(&self) -> Result<u32, BoxedError> {
        checksum_polyfill(self)
    }
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

struct Sled29(sled_0_29::Db);

impl Sled29 {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, BoxedError> {
        Ok(Self(sled_0_29::Db::open(path)?))
    }
}

impl SledAdapter for Sled29 {
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
        Ok(Box::new(Tree29(self.0.open_tree(name)?)))
    }

    fn checksum(&self) -> Result<u32, BoxedError> {
        Ok(self.0.checksum()?)
    }
}

fn checksum_polyfill(adapter: &dyn SledAdapter) -> Result<u32, BoxedError> {
    // Checksum was released in 0.29.0, so we replicate its function for earlier versions
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

fn version_detect(path: &Path) -> Result<Option<SledVersion>, Error> {
    let conf_path = path.join("conf");
    let mut file = OpenOptions::new().read(true).open(conf_path)?;
    let mut buf = vec![];
    file.read_to_end(&mut buf)?;
    if buf.len() <= 4 {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "configuration file too short").into(),
        );
    }
    let crc_vec = buf.split_off(buf.len() - 4);
    let crc_arr = (&crc_vec[..]).try_into().unwrap();
    let crc_expected = u32::from_le_bytes(crc_arr);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf);
    let crc_actual = hasher.finalize();
    assert_eq!(crc_actual, crc_expected);

    Ok(version_detect_config(&buf))
}

fn version_detect_config(buf: &[u8]) -> Option<SledVersion> {
    // can only detect 0.25 and higher, earlier databases have no version info
    let reader = BufReader::new(buf);

    let mut utf8_clean = true;
    let mut version_text = String::new();

    for line in reader.lines() {
        match line {
            Ok(line) => {
                if line.starts_with("version: ") {
                    version_text = String::from(&line[9..]);
                }
            }
            Err(_) => {
                utf8_clean = false;
                break;
            }
        }
    }

    if utf8_clean {
        if let Some(version) = SledVersion::from_text(&version_text) {
            return Some(version);
        } else {
            panic!("Unsupported version {}", version_text);
        }
    }

    if buf.len() > 16 {
        match buf[buf.len() - 16..] {
            [0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0, 0, 0, 0, 0, 0] => Some(SledVersion::Sled25),
            [0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0] => Some(SledVersion::Sled28),
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{main, migrate, version_detect_config, SledVersion};
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
    fn migrate_23_24() {
        let from_dir = PathBuf::from("db2324a");
        let to_dir = PathBuf::from("db2324b");

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_23::Db::start_default(&from_dir).unwrap();
        fill!(db, set, del);
        drop(db);

        migrate(&from_dir, SledVersion::Sled23, &to_dir, SledVersion::Sled24);

        let db = sled_0_24::Db::start_default(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn main_23_24() {
        let from_str = "db2324c";
        let to_str = "db2324d";
        let from_dir = PathBuf::from(from_str);
        let to_dir = PathBuf::from(to_str);

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_23::Db::start_default(&from_dir).unwrap();
        fill!(db, set, del);
        drop(db);

        main(
            vec![
                String::from("sled-migrate"),
                String::from("--inpath"),
                String::from(from_str),
                String::from("--inver"),
                String::from("0.23"),
                String::from("--outpath"),
                String::from(to_str),
                String::from("--outver"),
                String::from("0.24"),
            ]
            .into_iter(),
        );

        let db = sled_0_24::Db::start_default(to_dir).unwrap();
        check!(db);
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

        migrate(&from_dir, SledVersion::Sled24, &to_dir, SledVersion::Sled25);

        let db = sled_0_25::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn main_24_25() {
        let from_str = "db2425c";
        let to_str = "db2425d";
        let from_dir = PathBuf::from(from_str);
        let to_dir = PathBuf::from(to_str);

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_24::Db::start_default(&from_dir).unwrap();
        fill!(db, set, del);
        drop(db);

        main(
            vec![
                String::from("sled-migrate"),
                String::from("--inpath"),
                String::from(from_str),
                String::from("--inver"),
                String::from("0.24"),
                String::from("--outpath"),
                String::from(to_str),
                String::from("--outver"),
                String::from("0.25"),
            ]
            .into_iter(),
        );

        let db = sled_0_25::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn migrate_25_28() {
        let from_dir = PathBuf::from("db2528a");
        let to_dir = PathBuf::from("db2528b");

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_25::Db::open(&from_dir).unwrap();
        fill!(db);
        drop(db);

        migrate(&from_dir, SledVersion::Sled25, &to_dir, SledVersion::Sled28);

        let db = sled_0_28::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn main_25_28() {
        let from_str = "db2528c";
        let to_str = "db2528d";
        let from_dir = PathBuf::from(from_str);
        let to_dir = PathBuf::from(to_str);

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_25::Db::open(&from_dir).unwrap();
        fill!(db);
        drop(db);

        main(
            vec![
                String::from("sled-migrate"),
                String::from("--inpath"),
                String::from(from_str),
                String::from("--outpath"),
                String::from(to_str),
                String::from("--outver"),
                String::from("0.28"),
            ]
            .into_iter(),
        );

        let db = sled_0_28::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn migrate_28_29() {
        let from_dir = PathBuf::from("db2829a");
        let to_dir = PathBuf::from("db2829b");

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_28::Db::open(&from_dir).unwrap();
        fill!(db);
        drop(db);

        migrate(&from_dir, SledVersion::Sled28, &to_dir, SledVersion::Sled29);

        let db = sled_0_29::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn main_28_29() {
        let from_str = "db2829c";
        let to_str = "db2829d";
        let from_dir = PathBuf::from(from_str);
        let to_dir = PathBuf::from(to_str);

        let _ = remove_dir_all(&from_dir);
        let _ = remove_dir_all(&to_dir);

        let db = sled_0_28::Db::open(&from_dir).unwrap();
        fill!(db);
        drop(db);

        main(
            vec![
                String::from("sled-migrate"),
                String::from("--inpath"),
                String::from(from_str),
                String::from("--outpath"),
                String::from(to_str),
                String::from("--outver"),
                String::from("0.29"),
            ]
            .into_iter(),
        );

        let db = sled_0_29::Db::open(to_dir).unwrap();
        check!(db);
    }

    #[test]
    fn version_detection() {
        fn drop_last_four_bytes(buf: &[u8]) -> &[u8] {
            &buf[..buf.len() - 4]
        }

        assert_eq!(
            version_detect_config(drop_last_four_bytes(include_bytes!("data/conf23"))),
            None
        );
        assert_eq!(
            version_detect_config(drop_last_four_bytes(include_bytes!("data/conf24"))),
            None
        );
        assert_eq!(
            version_detect_config(drop_last_four_bytes(include_bytes!("data/conf25"))),
            Some(SledVersion::Sled25)
        );
        assert_eq!(
            version_detect_config(drop_last_four_bytes(include_bytes!("data/conf28"))),
            Some(SledVersion::Sled28)
        );
        assert_eq!(
            version_detect_config(drop_last_four_bytes(include_bytes!("data/conf29"))),
            Some(SledVersion::Sled29)
        );
    }
}
