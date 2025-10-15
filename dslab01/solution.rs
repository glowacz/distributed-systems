pub struct Fibonacci {
    // Add here any fields you need.
    n: usize,
    val: u128,
    next_val: u128,
}

impl Fibonacci {
    /// Create new `Fibonacci`.
    pub fn new() -> Fibonacci {
        Fibonacci { 
            n: 1, 
            val: 0, 
            next_val: 1 
        }
    }

    /// Calculate the n-th Fibonacci number.
    ///
    /// This shall not change the state of the iterator.
    /// The calculations shall wrap around at the boundary of u8.
    /// The calculations might be slow (recursive calculations are acceptable).
    pub fn fibonacci(n: usize) -> u8 {
        if n == 0 {
            return 0
        }
        if n == 1 {
            return 1
        }

        Fibonacci::fibonacci(n - 2).wrapping_add(Fibonacci::fibonacci(n - 1))
    }
}

impl Iterator for Fibonacci {
    type Item = u128;

    /// Calculate the next Fibonacci number.
    ///
    /// The first call to `next()` shall return the 0th Fibonacci number (i.e., `0`).
    /// The calculations shall not overflow and shall not wrap around. If the result
    /// doesn't fit u128, the sequence shall end (the iterator shall return `None`).
    /// The calculations shall be fast (recursive calculations are **un**acceptable).
    fn next(&mut self) -> Option<Self::Item> {
        let old_val = self.val;
        let next_val = self.val.checked_add(self.next_val);
        // println!("Prev val is {0}", self.prev_val);
        // println!("val is {0}", self.val);
        // println!("New val is {0:?}", new_val);

        self.val = self.next_val;
        self.next_val = next_val?;
        self.n += 1;

        // println!("Incrementing iterator, now at n {0:?}", self.n);


        Some(old_val)
    }
}
