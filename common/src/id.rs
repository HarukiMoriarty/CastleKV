use serde::{Deserialize, Serialize};
use std::{fmt, mem, str};

macro_rules! primitive_id {
    ($name:ident, $repr_type:ty) => {
        #[derive(
            Clone,
            Copy,
            Default,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        pub struct $name($repr_type);

        impl From<$name> for $repr_type {
            fn from(node_id: $name) -> Self {
                node_id.0
            }
        }

        impl From<$repr_type> for $name {
            fn from(repr: $repr_type) -> Self {
                Self(repr)
            }
        }

        impl str::FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let id = s.parse::<$repr_type>()?;
                Ok(Self(id))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

macro_rules! from_id_to_primitive {
    ($name:ident, $primitive:ty) => {
        impl From<$name> for $primitive {
            fn from(id: $name) -> Self {
                id.0 as $primitive
            }
        }
    };
}

primitive_id!(NodeId, u32);
from_id_to_primitive!(NodeId, u64);
from_id_to_primitive!(NodeId, usize);
impl NodeId {
    pub const BIT_LENGTH: usize = mem::size_of::<u32>() * 8;
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(NodeId, u32);

impl TransactionId {
    pub const INVALID: TransactionId = Self(NodeId(0), 0);

    pub fn new(node_id: NodeId, counter: u32) -> Self {
        Self(node_id, counter)
    }

    pub fn node_id(self) -> NodeId {
        self.0
    }

    pub fn counter(self) -> u32 {
        self.1
    }
}

impl fmt::Debug for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnId({},{})", self.0, self.1)
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.0, self.1)
    }
}

impl Serialize for TransactionId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl From<u64> for TransactionId {
    fn from(raw: u64) -> Self {
        Self(
            NodeId((raw & 0xFFFF_FFFF) as u32),
            (raw >> NodeId::BIT_LENGTH) as u32,
        )
    }
}

impl From<TransactionId> for u64 {
    fn from(txn_id: TransactionId) -> Self {
        (txn_id.1 as u64) << NodeId::BIT_LENGTH | ((txn_id.0).0 as u64)
    }
}

pub type CommandId = u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_id() {
        let txn_id = TransactionId::new(NodeId(1), 2);
        assert_eq!(txn_id.node_id(), NodeId(1));
        assert_eq!(txn_id.counter(), 2);
        assert_eq!(txn_id.to_string(), "(1,2)");
        assert_eq!(u64::from(txn_id), 0x0000_0002_0000_0001);

        let txn_id = TransactionId::from(0x0000_0002_0000_0001);
        assert_eq!(txn_id.node_id(), NodeId(1));
        assert_eq!(txn_id.counter(), 2);
        assert_eq!(txn_id.to_string(), "(1,2)");
        assert_eq!(u64::from(txn_id), 0x0000_0002_0000_0001);
    }
}
