//! Type-keyed dependency-injection container for Sayiir workflows.
//!
//! `Deps` lets you register cloneable values once and resolve them from anywhere
//! a `&Deps` is available — most notably inside `#[task]`-generated `from_deps`
//! constructors and the `workflow!` macro's `deps:` field.
//!
//! # Quick Example
//!
//! ```
//! use sayiir_di::Deps;
//! use std::sync::Arc;
//!
//! #[derive(Clone)]
//! struct HttpClient;
//!
//! let deps = Deps::builder()
//!     .insert(Arc::new(HttpClient))
//!     .build();
//!
//! let client: Arc<HttpClient> = deps.expect();
//! ```
//!
//! # Lookup Rules
//!
//! Resolution is by **exact `TypeId`**. Insert `Arc<HttpClient>` → resolve
//! `Arc<HttpClient>`. There is no coercion to traits or supertypes.
//!
//! Stored values must be `Send + Sync + 'static`, and `get` / `expect` /
//! `try_get` require `Clone` because the container owns one copy per type.

#![deny(missing_docs)]
#![deny(clippy::pedantic)]
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr
)]

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;

/// A type-keyed container of cloneable services.
///
/// Built with [`Deps::builder`] and read by type via [`Deps::get`], [`Deps::expect`],
/// or [`Deps::try_get`]. Used by `#[task]`-generated `from_deps` constructors.
#[derive(Default)]
pub struct Deps {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Deps {
    /// Create an empty container.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Start a builder.
    #[must_use]
    pub fn builder() -> DepsBuilder {
        DepsBuilder { inner: Deps::new() }
    }

    /// Resolve `T`, returning a fresh clone, or `None` if the type was never inserted.
    #[must_use]
    pub fn get<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<T>())
            .cloned()
    }

    /// Resolve `T`, returning a fresh clone, or [`MissingDep`] describing the missing type.
    ///
    /// # Errors
    ///
    /// Returns [`MissingDep`] if no value of type `T` was inserted into the container.
    pub fn try_get<T>(&self) -> Result<T, MissingDep>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.get::<T>().ok_or_else(MissingDep::of::<T>)
    }

    /// Resolve `T`, panicking with the type name on miss.
    ///
    /// Use [`Deps::try_get`] (or [`Deps::get`]) when a missing dependency should
    /// be a recoverable error. `expect` is meant for codegen sites that have
    /// *already* been verified by `verify_deps`.
    ///
    /// # Panics
    ///
    /// Panics if no value of type `T` was inserted into the container.
    #[must_use]
    pub fn expect<T>(&self) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        match self.get::<T>() {
            Some(v) => v,
            None => missing_panic(type_name::<T>()),
        }
    }

    /// Returns `true` if a value of type `T` is present.
    #[must_use]
    pub fn contains<T>(&self) -> bool
    where
        T: 'static,
    {
        self.map.contains_key(&TypeId::of::<T>())
    }

    /// Number of registered types.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether no types are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl fmt::Debug for Deps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deps")
            .field("registered_types", &self.map.len())
            .finish()
    }
}

/// Builder for [`Deps`]. Use [`Deps::builder`] to create one.
pub struct DepsBuilder {
    inner: Deps,
}

impl DepsBuilder {
    /// Insert (or replace) a value of type `T`.
    ///
    /// `T` is the key — if two `insert` calls share the same type, the later
    /// one wins.
    #[must_use]
    pub fn insert<T>(mut self, dep: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.inner.map.insert(TypeId::of::<T>(), Box::new(dep));
        self
    }

    /// Finalize and return the [`Deps`] container.
    #[must_use]
    pub fn build(self) -> Deps {
        self.inner
    }
}

impl Default for DepsBuilder {
    fn default() -> Self {
        Deps::builder()
    }
}

/// A dependency that was requested from a [`Deps`] container but not present.
///
/// `#[task]`-generated `verify_deps` returns a `Vec<MissingDep>`; the
/// `workflow!` macro converts those into build errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingDep {
    /// The `std::any::type_name` of the missing type.
    pub type_name: &'static str,
}

impl MissingDep {
    /// Build a `MissingDep` for type `T`.
    #[must_use]
    pub fn of<T: ?Sized + 'static>() -> Self {
        Self {
            type_name: type_name::<T>(),
        }
    }
}

impl fmt::Display for MissingDep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "missing dependency `{}` in Deps container",
            self.type_name
        )
    }
}

impl std::error::Error for MissingDep {}

#[cold]
#[inline(never)]
#[allow(clippy::panic)]
fn missing_panic(type_name: &'static str) -> ! {
    // Documented panic behaviour of `Deps::expect`. `verify_deps` is the
    // recoverable path; callers reach this function only when they skipped it.
    panic!(
        "Deps::expect: missing dependency `{type_name}` (verify_deps should have caught this at workflow build time)"
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ServiceA(u32);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ServiceB(&'static str);

    #[test]
    fn insert_and_get_concrete() {
        let deps = Deps::builder().insert(ServiceA(7)).build();
        assert_eq!(deps.get::<ServiceA>(), Some(ServiceA(7)));
    }

    #[test]
    fn insert_arc_keeps_arc_key() {
        let deps = Deps::builder().insert(Arc::new(ServiceA(7))).build();
        // Arc<ServiceA> is the registered key, not ServiceA.
        assert!(deps.contains::<Arc<ServiceA>>());
        assert!(!deps.contains::<ServiceA>());
        let resolved: Arc<ServiceA> = deps.expect();
        assert_eq!(*resolved, ServiceA(7));
    }

    #[test]
    fn multiple_types_coexist() {
        let deps = Deps::builder()
            .insert(ServiceA(1))
            .insert(ServiceB("hi"))
            .build();
        assert_eq!(deps.len(), 2);
        assert_eq!(deps.get::<ServiceA>(), Some(ServiceA(1)));
        assert_eq!(deps.get::<ServiceB>(), Some(ServiceB("hi")));
    }

    #[test]
    fn missing_type_returns_none() {
        let deps = Deps::new();
        assert!(deps.get::<ServiceA>().is_none());
    }

    #[test]
    fn try_get_reports_type_name() {
        let deps = Deps::new();
        let err = deps.try_get::<ServiceA>().unwrap_err();
        assert!(err.type_name.contains("ServiceA"));
    }

    #[test]
    fn last_insert_wins_for_same_type() {
        let deps = Deps::builder()
            .insert(ServiceA(1))
            .insert(ServiceA(2))
            .build();
        assert_eq!(deps.get::<ServiceA>(), Some(ServiceA(2)));
    }

    #[test]
    fn expect_returns_value() {
        let deps = Deps::builder().insert(ServiceA(42)).build();
        let value: ServiceA = deps.expect();
        assert_eq!(value, ServiceA(42));
    }

    #[test]
    #[should_panic(expected = "missing dependency")]
    fn expect_panics_with_message() {
        let deps = Deps::new();
        let _: ServiceA = deps.expect();
    }

    #[test]
    fn missing_dep_display() {
        let m = MissingDep::of::<ServiceA>();
        let rendered = format!("{m}");
        assert!(rendered.contains("ServiceA"));
        assert!(rendered.contains("missing dependency"));
    }

    #[test]
    fn empty_and_len() {
        let mut deps = Deps::new();
        assert!(deps.is_empty());
        deps = Deps::builder().insert(ServiceA(0)).build();
        assert!(!deps.is_empty());
        assert_eq!(deps.len(), 1);
    }
}
