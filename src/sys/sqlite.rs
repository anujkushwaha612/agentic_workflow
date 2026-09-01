//! Safe wrapper over the system SQLite (`libsqlite3.so.0`, linked by exact
//! filename — no dev headers needed). This is the only place in the crate with
//! `unsafe`, and each block is a direct C call with owned data on either side.
//!
//! Why hand-rolled instead of `rusqlite`: the sandbox cannot reach crates.io
//! (RUST_MIGRATION_PLAN.md §0). The surface used by the journal is small:
//! open/exec/prepare/bind/step/column/finalize.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fmt;
use std::path::Path;

#[allow(non_camel_case_types)]
type sqlite3 = c_void;
#[allow(non_camel_case_types)]
type sqlite3_stmt = c_void;

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_OPEN_READWRITE: c_int = 0x2;
const SQLITE_OPEN_CREATE: c_int = 0x4;
const SQLITE_OPEN_NOMUTEX: c_int = 0x8000;

// Link against the versioned runtime library directly: the sandbox has no
// unversioned `libsqlite3.so` dev symlink, only `libsqlite3.so.0`. The `:`
// prefix passes the exact filename to the linker (works with rust-lld).
#[link(name = ":libsqlite3.so.0", kind = "dylib")]
extern "C" {
    #[link_name = "sqlite3_open_v2"]
    fn sqlite3_open_v2(
        filename: *const c_char,
        db: *mut *mut sqlite3,
        flags: c_int,
        vfs: *const c_char,
    ) -> c_int;
    #[link_name = "sqlite3_close_v2"]
    fn sqlite3_close_v2(db: *mut sqlite3) -> c_int;
    #[link_name = "sqlite3_exec"]
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        cb: *const c_void,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    #[link_name = "sqlite3_prepare_v2"]
    fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const c_char,
        nbyte: c_int,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const c_char,
    ) -> c_int;
    #[link_name = "sqlite3_bind_text"]
    fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        idx: c_int,
        val: *const c_char,
        n: c_int,
        destructor: *const c_void,
    ) -> c_int;
    #[link_name = "sqlite3_bind_null"]
    fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    #[link_name = "sqlite3_bind_double"]
    fn sqlite3_bind_double(stmt: *mut sqlite3_stmt, idx: c_int, val: f64) -> c_int;
    #[link_name = "sqlite3_bind_int64"]
    fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, val: i64) -> c_int;
    #[link_name = "sqlite3_step"]
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    #[link_name = "sqlite3_finalize"]
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
    #[link_name = "sqlite3_column_count"]
    fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> c_int;
    #[link_name = "sqlite3_column_name"]
    fn sqlite3_column_name(stmt: *mut sqlite3_stmt, i: c_int) -> *const c_char;
    #[link_name = "sqlite3_column_type"]
    fn sqlite3_column_type(stmt: *mut sqlite3_stmt, i: c_int) -> c_int;
    #[link_name = "sqlite3_column_int64"]
    fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, i: c_int) -> i64;
    #[link_name = "sqlite3_column_double"]
    fn sqlite3_column_double(stmt: *mut sqlite3_stmt, i: c_int) -> f64;
    #[link_name = "sqlite3_column_text"]
    fn sqlite3_column_text(stmt: *mut sqlite3_stmt, i: c_int) -> *const u8;
    #[link_name = "sqlite3_column_bytes"]
    fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, i: c_int) -> c_int;
    #[link_name = "sqlite3_last_insert_rowid"]
    fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64;
    #[link_name = "sqlite3_changes"]
    fn sqlite3_changes(db: *mut sqlite3) -> i32;
    #[link_name = "sqlite3_extended_errcode"]
    fn sqlite3_extended_errcode(db: *mut sqlite3) -> c_int;
    #[link_name = "sqlite3_errmsg"]
    fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    #[link_name = "sqlite3_libversion"]
    fn sqlite3_libversion() -> *const c_char;
}

/// SQLITE_CONSTRAINT_PRIMARYKEY and friends: base code 19.
const SQLITE_CONSTRAINT_BASE: c_int = 19;

pub fn libversion() -> &'static str {
    unsafe {
        CStr::from_ptr(sqlite3_libversion() as *const c_char)
            .to_str()
            .unwrap_or("?")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    pub code: i32,
    pub message: String,
    pub is_constraint: bool,
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sqlite error {} (constraint={}): {}",
            self.code, self.is_constraint, self.message
        )
    }
}
impl std::error::Error for SqlError {}

pub type Result<T> = std::result::Result<T, SqlError>;

pub struct Db {
    raw: *mut sqlite3,
}

// The journal is explicitly single-owner/single-threaded (as in Python:
// "Not thread-safe by design: a single kernel owns it").
unsafe impl Send for Db {}

impl Db {
    pub fn open(path: &Path) -> Result<Db> {
        let cpath = CString::new(path.to_string_lossy().as_bytes()).map_err(|_| SqlError {
            code: -1,
            message: "path contains NUL".into(),
            is_constraint: false,
        })?;
        let mut raw: *mut sqlite3 = std::ptr::null_mut();
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
        let rc = unsafe { sqlite3_open_v2(cpath.as_ptr(), &mut raw, flags, std::ptr::null()) };
        if rc != SQLITE_OK {
            let msg = unsafe {
                CStr::from_ptr(sqlite3_errmsg(raw) as *const c_char)
                    .to_string_lossy()
                    .into_owned()
            };
            if !raw.is_null() {
                unsafe { sqlite3_close_v2(raw) };
            }
            return Err(SqlError {
                code: rc as i32,
                message: msg,
                is_constraint: false,
            });
        }
        let db = Db { raw };
        db.exec("PRAGMA busy_timeout=5000")?;
        Ok(db)
    }

    pub fn exec(&self, sql: &str) -> Result<()> {
        let csql = CString::new(sql).map_err(|_| SqlError {
            code: -1,
            message: "NUL in sql".into(),
            is_constraint: false,
        })?;
        let rc = unsafe {
            sqlite3_exec(
                self.raw,
                csql.as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(self.err(rc));
        }
        Ok(())
    }

    fn err(&self, rc: c_int) -> SqlError {
        let code = unsafe { sqlite3_extended_errcode(self.raw) } as i32;
        let msg = unsafe {
            CStr::from_ptr(sqlite3_errmsg(self.raw) as *const c_char)
                .to_string_lossy()
                .into_owned()
        };
        SqlError {
            code,
            message: msg,
            is_constraint: rc == SQLITE_CONSTRAINT_BASE
                || (code / 1000) == (SQLITE_CONSTRAINT_BASE / 1000)
                    && code % 1000 == SQLITE_CONSTRAINT_BASE,
        }
    }

    /// Prepare, bind, run to completion. Returns rows if the statement yields.
    pub fn run(&self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        let stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        let mut rows = Vec::new();
        let raw = stmt.raw;
        loop {
            let rc = unsafe { sqlite3_step(raw) };
            if rc == SQLITE_ROW {
                rows.push(unsafe { read_row(raw) });
            } else if rc == SQLITE_DONE {
                break;
            } else {
                let e = self.err(rc);
                drop(stmt);
                return Err(e);
            }
        }
        Ok(rows)
    }

    pub fn prepare<'a>(&self, sql: &'a str) -> Result<Stmt<'a>> {
        let csql = CString::new(sql).map_err(|_| SqlError {
            code: -1,
            message: "NUL in sql".into(),
            is_constraint: false,
        })?;
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(self.raw, csql.as_ptr(), -1, &mut stmt, std::ptr::null_mut())
        };
        if rc != SQLITE_OK {
            return Err(self.err(rc));
        }
        Ok(Stmt {
            raw: stmt,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { sqlite3_last_insert_rowid(self.raw) }
    }
    pub fn changes(&self) -> i32 {
        unsafe { sqlite3_changes(self.raw) }
    }
    pub fn execute(&self, sql: &str, params: &[Param]) -> Result<usize> {
        let n = self.run(sql, params)?.len();
        let _ = n;
        Ok(self.changes() as usize)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { sqlite3_close_v2(self.raw) };
        }
    }
}

/// Borrowed statement: tied to the Db lifetime at the type level.
pub struct Stmt<'a> {
    raw: *mut sqlite3_stmt,
    _marker: std::marker::PhantomData<&'a Db>,
}

#[derive(Clone, Debug)]
pub enum Param {
    Null,
    Text(String),
    Int(i64),
    Real(f64),
}

impl From<&str> for Param {
    fn from(s: &str) -> Param {
        Param::Text(s.to_string())
    }
}
impl From<String> for Param {
    fn from(s: String) -> Param {
        Param::Text(s)
    }
}
impl From<i64> for Param {
    fn from(i: i64) -> Param {
        Param::Int(i)
    }
}
impl From<f64> for Param {
    fn from(f: f64) -> Param {
        Param::Real(f)
    }
}
impl<T: Into<Param>> From<Option<T>> for Param {
    fn from(o: Option<T>) -> Param {
        match o {
            Some(v) => v.into(),
            None => Param::Null,
        }
    }
}

/// SQLITE_TRANSIENT (-1) as the string destructor: sqlite copies the bytes.
const SQLITE_TRANSIENT: *const c_void = -1isize as *const c_void;

impl<'a> Stmt<'a> {
    pub fn bind_all(&self, params: &[Param]) -> Result<()> {
        for (i, p) in params.iter().enumerate() {
            let idx = (i + 1) as c_int;
            let rc = match p {
                Param::Null => unsafe { sqlite3_bind_null(self.raw, idx) },
                Param::Text(s) => unsafe {
                    sqlite3_bind_text(
                        self.raw,
                        idx,
                        s.as_ptr().cast(),
                        s.len() as c_int,
                        SQLITE_TRANSIENT,
                    )
                },
                Param::Int(v) => unsafe { sqlite3_bind_int64(self.raw, idx, *v) },
                Param::Real(v) => unsafe { sqlite3_bind_double(self.raw, idx, *v) },
            };
            if rc != SQLITE_OK {
                return Err(SqlError {
                    code: rc as i32,
                    message: format!("bind {} failed", i + 1),
                    is_constraint: false,
                });
            }
        }
        Ok(())
    }

    /// Step once: Ok(true) = row available via `row()`, Ok(false) = done.
    pub fn step(&self) -> Result<bool> {
        let rc = unsafe { sqlite3_step(self.raw) };
        if rc == SQLITE_ROW {
            return Ok(true);
        }
        if rc == SQLITE_DONE {
            return Ok(false);
        }
        Err(SqlError {
            code: rc as i32,
            message: format!("step rc={rc}"),
            is_constraint: rc == SQLITE_CONSTRAINT_BASE,
        })
    }

    pub fn row(&self) -> Row {
        unsafe { read_row(self.raw) }
    }

    pub fn raw(&self) -> *mut sqlite3_stmt {
        self.raw
    }
}

impl Drop for Stmt<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { sqlite3_finalize(self.raw) };
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Row {
    pub cols: Vec<String>,
    pub vals: Vec<Val>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl Row {
    pub fn get(&self, name: &str) -> &Val {
        self.cols
            .iter()
            .position(|c| c == name)
            .map(|i| &self.vals[i])
            .unwrap_or(&Val::Null)
    }
    pub fn text(&self, name: &str) -> String {
        match self.get(name) {
            Val::Text(s) => s.clone(),
            Val::Int(i) => i.to_string(),
            Val::Real(f) => crate::sys::json::py_float_repr(*f),
            Val::Null => String::new(),
        }
    }
    pub fn opt_text(&self, name: &str) -> Option<String> {
        match self.get(name) {
            Val::Text(s) => Some(s.clone()),
            _ => None,
        }
    }
    pub fn int(&self, name: &str) -> i64 {
        match self.get(name) {
            Val::Int(i) => *i,
            Val::Real(f) => *f as i64,
            Val::Text(s) => s.parse().unwrap_or(0),
            Val::Null => 0,
        }
    }
    pub fn real(&self, name: &str) -> f64 {
        match self.get(name) {
            Val::Real(f) => *f,
            Val::Int(i) => *i as f64,
            Val::Text(s) => s.parse().unwrap_or(0.0),
            Val::Null => 0.0,
        }
    }
    pub fn is_null(&self, name: &str) -> bool {
        matches!(self.get(name), Val::Null)
    }
}

unsafe fn read_row(raw: *mut sqlite3_stmt) -> Row {
    let n = sqlite3_column_count(raw);
    let mut cols = Vec::with_capacity(n as usize);
    let mut vals = Vec::with_capacity(n as usize);
    for i in 0..n {
        let name = sqlite3_column_name(raw, i);
        cols.push(if name.is_null() {
            String::new()
        } else {
            CStr::from_ptr(name as *const c_char)
                .to_string_lossy()
                .into_owned()
        });
        let ty = sqlite3_column_type(raw, i);
        let val = match ty {
            1 => Val::Int(sqlite3_column_int64(raw, i)),
            2 => Val::Real(sqlite3_column_double(raw, i)),
            3 => {
                let p = sqlite3_column_text(raw, i);
                let len = sqlite3_column_bytes(raw, i) as usize;
                if p.is_null() {
                    Val::Null
                } else {
                    Val::Text(
                        String::from_utf8_lossy(std::slice::from_raw_parts(p, len)).into_owned(),
                    )
                }
            }
            4 => Val::Int(sqlite3_column_int64(raw, i)),
            _ => Val::Null,
        };
        vals.push(val);
    }
    Row { cols, vals }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_constraint() {
        let dir = std::env::temp_dir().join(format!("arena-sqlite-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Db::open(&dir.join("t.db")).unwrap();
        db.exec("CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES(?, ?)",
            &[Param::Text("a".into()), Param::Int(1)],
        )
        .unwrap();
        let rows = db
            .run("SELECT v FROM t WHERE k=?", &[Param::Text("a".into())])
            .unwrap();
        assert_eq!(rows[0].int("v"), 1);
        // duplicate insert -> constraint error
        let err = db.execute(
            "INSERT INTO t VALUES(?, ?)",
            &[Param::Text("a".into()), Param::Int(2)],
        );
        match err {
            Err(e) => assert!(e.is_constraint, "{e}"),
            Ok(_) => panic!("expected constraint violation"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
