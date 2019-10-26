use std::env::args;

fn main() {
    sled_migrate::main(args());
    println!("Migration successful");
}
