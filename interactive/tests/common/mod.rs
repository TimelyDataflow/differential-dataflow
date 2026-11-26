//! Test helpers shared by several test crates.

/// A host kernel from its name, shapes and body.
pub struct Kernel { name: &'static str, input: corgi::Shape, output: corgi::Shape, body: fn(corgi::Value) -> Result<corgi::Value, String> }

impl corgi::HostKernel for Kernel {
    fn name(&self) -> &str { self.name }
    fn input(&self) -> &corgi::Shape { &self.input }
    fn output(&self) -> &corgi::Shape { &self.output }
    fn eval(&self, input: corgi::Value) -> Result<corgi::Value, String> { (self.body)(input) }
}

/// Register the function `name` of arguments `args`: its input is `()`, or the tuple of them.
pub fn register(name: &'static str, args: Vec<corgi::Shape>, output: corgi::Shape, body: fn(corgi::Value) -> Result<corgi::Value, String>) -> interactive::lambda::Function {
    let input = if args.is_empty() { corgi::Shape::Unit } else { corgi::Shape::Prod(args) };
    let f: interactive::lambda::Function = std::sync::Arc::new(Kernel { name, input, output, body });
    interactive::lambda::register(std::sync::Arc::clone(&f));
    f
}

/// The functions `registered.ddp` calls. Registering again replaces, so every test may do it.
pub fn register_test_functions() {
    use corgi::{Shape, Value};
    let (int, list) = (|| Shape::Int, |s| Shape::List(Box::new(s)));
    /// The first argument's integers.
    fn first(input: Value) -> Result<Vec<i64>, String> { input.into_prod("argument")?.remove(0).into_i64("argument") }
    // A cell's children: two, while the cell is small and positive; none after.
    register("grow", vec![int()], list(Shape::Prod(vec![int(), int()])), |input| {
        let (mut ends, mut kids, mut parents) = (Vec::new(), Vec::new(), Vec::new());
        for n in first(input)? {
            if (1..20).contains(&n) { kids.extend([2 * n, 2 * n + 1]); parents.extend([n, n]) }
            ends.push(kids.len());
        }
        Ok(Value::List(ends.into(), Box::new(Value::Prod(vec![Value::int(kids), Value::int(parents)]))))
    });
    // A float from a pair of ints.
    register("blend", vec![Shape::Prod(vec![int(), int()])], Shape::Float, |input| {
        let pair = input.into_prod("blend")?.remove(0).into_prod("blend")?;
        let (a, b) = (pair[0].as_i64("blend")?, pair[1].as_i64("blend")?);
        Ok(Value::f64(a.iter().zip(b.iter()).map(|(&a, &b)| (a as f64).sqrt() - b as f64 / 3.0).collect()))
    });
    // A nested shape: (n * 3, [divisors of n under 5]).
    register("describe", vec![int()], Shape::Prod(vec![Shape::Prod(vec![int()]), list(int())]), |input| {
        let ns = first(input)?;
        let (mut ends, mut divisors) = (Vec::new(), Vec::new());
        for &n in &ns {
            divisors.extend((1..5).filter(|d| n % d == 0));
            ends.push(divisors.len());
        }
        let tripled = Value::Prod(vec![Value::int(ns.iter().map(|n| 3 * n))]);
        Ok(Value::Prod(vec![tripled, Value::List(ends.into(), Box::new(Value::int(divisors)))]))
    });
    // No arguments: the call passes a `Unit` column, so it still runs once per row.
    register("seven", vec![], int(), |input| Ok(Value::int(std::iter::repeat_n(7, input.len()))));
    register("double", vec![int()], int(), |input| Ok(Value::int(first(input)?.into_iter().map(|x| 2 * x))));
}
