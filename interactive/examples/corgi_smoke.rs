//! Rung-0 smoke test: prove we can drive corgi from inside the DDIR crate.
//! Build a corgi program, run it over a column, read the result back.
//! `~/.cargo/bin/cargo run -p interactive --example corgi_smoke`

fn main() {
    // A columnar program: per-row x -> x + 100. corgi ops are ALREADY columnar over a Prim
    // column (the column IS the batch of rows), so no `map` wrapper — the op is the vectorized map.
    // (`map`/MapList is only for mapping over the inner lists of a List column.)
    let g = corgi::parse_ml("input add_u64 100").expect("parse");
    let input = corgi::Value::u64(vec![1, 2, 3, 40, 900]);
    let out = corgi::eval_graph(&g, input);
    println!("corgi map(+100) over [1,2,3,40,900] = {}", corgi::show(&out));

    // Shape inference (the dynamic-typing primitive we lean on): observe data -> Shape.
    let sample = corgi::Value::u64(vec![7, 8, 9]);
    println!("inferred shape of u64 column = {:?}", corgi::shape_of_value(&sample));

    // Static shape check of the program against that input shape.
    match corgi::shape_of(&g, &corgi::shape_of_value(&sample)) {
        Ok(s) => println!("program output shape = {:?}", s),
        Err(e) => println!("shape error: {e}"),
    }
}
