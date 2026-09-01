//! Time source. Virtual (deterministic, chaos/replay) or wall clock — the
//! kernel never reads the system clock directly, which is what makes replay
//! equality testable (`arena/clock.py`).

use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum Clock {
    /// `now() == ticks * step`, advances deterministically per tick.
    Virtual { step: f64, current: f64 },
    /// Real seconds since kernel construction.
    Wall { origin: f64 },
}

fn wall_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

impl Clock {
    pub fn virtual_(step: f64) -> Clock {
        Clock::Virtual { step, current: 0.0 }
    }
    pub fn wall() -> Clock {
        Clock::Wall { origin: wall_now() }
    }
    pub fn make(mode: &str, step: f64) -> Result<Clock, String> {
        match mode {
            "virtual" => Ok(Clock::virtual_(step)),
            "wall" => Ok(Clock::wall()),
            other => Err(format!("unknown clock mode {other:?}")),
        }
    }
    pub fn now(&self) -> f64 {
        match self {
            Clock::Virtual { current, .. } => *current,
            Clock::Wall { origin } => wall_now() - *origin,
        }
    }
    pub fn tick(&mut self) -> f64 {
        match self {
            Clock::Virtual { step, current } => {
                *current += *step;
                *current
            }
            Clock::Wall { origin } => wall_now() - *origin,
        }
    }
    pub fn advance(&mut self, seconds: f64) -> f64 {
        match self {
            Clock::Virtual { current, .. } => {
                *current += seconds;
                *current
            }
            Clock::Wall { .. } => self.now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_clock_is_deterministic() {
        let mut c = Clock::virtual_(0.01);
        assert_eq!(c.now(), 0.0);
        for _ in 0..3 {
            c.tick();
        }
        let a = c.now();
        let mut d = Clock::virtual_(0.01);
        for _ in 0..3 {
            d.tick();
        }
        assert_eq!(a, d.now());
        c.advance(1.0);
        assert_eq!(c.now(), a + 1.0);
    }
}
