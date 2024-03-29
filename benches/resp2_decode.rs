use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn fibonacci(n: u64) -> u64 {
  match n {
    0 => 1,
    1 => 1,
    n => fibonacci(n - 1) + fibonacci(n - 2),
  }
}

fn owned(c: &mut Criterion) {
  c.bench_function("fib 15", |b| b.iter(|| fibonacci(black_box(15))));
  c.bench_function("fib 10", |b| b.iter(|| fibonacci(black_box(15))));
}

#[cfg(feature = "bytes")]
fn bytes(c: &mut Criterion) {
  c.bench_function("fib 15", |b| b.iter(|| fibonacci(black_box(15))));
  c.bench_function("fib 10", |b| b.iter(|| fibonacci(black_box(10))));
}

criterion_group!(bench_owned, owned);
#[cfg(feature = "bytes")]
criterion_group!(bench_bytes, bytes);

#[cfg(feature = "bytes")]
criterion_main!(bench_owned, bench_bytes);
#[cfg(not(feature = "bytes"))]
criterion_main!(bench_owned);
