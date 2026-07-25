fn main() {
    println!(
        "cargo:rustc-env=RHIZA_BENCH_CARGO_PROFILE={}",
        std::env::var("PROFILE").expect("Cargo must provide PROFILE")
    );
    println!(
        "cargo:rustc-env=RHIZA_BENCH_CARGO_OPT_LEVEL={}",
        std::env::var("OPT_LEVEL").expect("Cargo must provide OPT_LEVEL")
    );
}
