use std::fmt;

#[derive(Debug)]
pub enum YamlError {
    /// YAML parsing or deserialization failed.
    Parse(String),
    /// The workflow definition is invalid (missing fields, bad references, etc.).
    Compile(String),
    /// A `JMESPath` expression failed to evaluate.
    JmesPath(String),
    /// A handler referenced by a task step was not found in the user registry.
    MissingHandler(String),
    /// Wrapper for core `BuildError` during `to_runnable()`.
    Build(sayiir_core::error::BuildError),
}

impl fmt::Display for YamlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(msg) => write!(f, "YAML parse error: {msg}"),
            Self::Compile(msg) => write!(f, "compile error: {msg}"),
            Self::JmesPath(msg) => write!(f, "JMESPath error: {msg}"),
            Self::MissingHandler(id) => write!(f, "handler '{id}' not found in registry"),
            Self::Build(e) => write!(f, "build error: {e}"),
        }
    }
}

impl std::error::Error for YamlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Build(e) => Some(e),
            _ => None,
        }
    }
}

impl From<sayiir_core::error::BuildError> for YamlError {
    fn from(e: sayiir_core::error::BuildError) -> Self {
        Self::Build(e)
    }
}

impl From<serde_yml::Error> for YamlError {
    fn from(e: serde_yml::Error) -> Self {
        Self::Parse(e.to_string())
    }
}
