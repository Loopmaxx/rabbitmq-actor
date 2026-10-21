use lapin::ExchangeKind;
use serde::Deserialize;

/// An exchange that is declared on every (re)connect.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ExchangeConfig {
    /// Exchange name. The default exchange and `amq.*` exchanges cannot be declared.
    pub name: String,
    /// Exchange type, `topic` by default.
    #[serde(default)]
    pub kind: ExchangeType,
    /// Survives a broker restart. Default `true`.
    #[serde(default = "default_true")]
    pub durable: bool,
    /// Deleted when the last queue is unbound. Default `false`.
    #[serde(default)]
    pub auto_delete: bool,
}

impl ExchangeConfig {
    /// A durable exchange of the given type.
    pub fn new(name: impl Into<String>, kind: ExchangeType) -> Self {
        Self {
            name: name.into(),
            kind,
            durable: true,
            auto_delete: false,
        }
    }
}

/// Exchange type.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ExchangeType {
    /// Routes by exact routing key.
    Direct,
    /// Routes to all bound queues.
    Fanout,
    /// Routes by routing key pattern (`*`, `#`). The default.
    #[default]
    Topic,
    /// Routes by message headers.
    Headers,
}

impl ExchangeType {
    pub(crate) fn to_lapin(self) -> ExchangeKind {
        match self {
            Self::Direct => ExchangeKind::Direct,
            Self::Fanout => ExchangeKind::Fanout,
            Self::Topic => ExchangeKind::Topic,
            Self::Headers => ExchangeKind::Headers,
        }
    }
}

/// Binds a queue to an exchange with one or more routing keys.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BindingConfig {
    /// Exchange the queue is bound to.
    pub exchange: String,
    /// One binding is created per routing key. Must not be empty.
    pub routing_keys: Vec<String>,
}

impl BindingConfig {
    /// Binds to `exchange` with the given routing keys.
    pub fn new(
        exchange: impl Into<String>,
        routing_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            routing_keys: routing_keys.into_iter().map(Into::into).collect(),
        }
    }
}

fn default_true() -> bool {
    true
}
