use bytes_utils::Str;
use rand::Rng;
// TODO fix for no std imports
use std::{
  collections::{BTreeMap, BTreeSet},
  fmt,
  fmt::{write, Formatter},
  sync::Arc,
};

// TODO
///
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Addr {
  host: Str,
  port: u16,
}

#[cfg(feature = "std")]
impl fmt::Display for Addr {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}:{}", self.host, self.port)
  }
}

/// A slot range and associated cluster node information from the `CLUSTER SLOTS` command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SlotRange {
  /// The start of the hash slot range.
  pub start:    u16,
  /// The end of the hash slot range.
  pub end:      u16,
  /// The primary server owner.
  pub primary:  Addr,
  /// The internal ID assigned by the server.
  pub id:       Str,
  /// Replica nodes that own this range.
  pub replicas: Vec<Addr>,
}

/// A cluster routing interface.
#[derive(Debug, Clone)]
pub struct ClusterRouting {
  data: Vec<SlotRange>,
}

impl Default for ClusterRouting {
  fn default() -> Self {
    ClusterRouting { data: Vec::new() }
  }
}

impl ClusterRouting {
  /// Create a new routing table from the result of the `CLUSTER SLOTS` command.
  ///
  /// The `default_host` value refers to the server that provided the response.
  pub fn from_resp3_cluster_slots<S: Into<Str>>(
    value: Resp3Frame,
    default_host: S,
  ) -> Result<Self, RedisProtocolError> {
    let default_host = default_host.into();
    let mut data = parse_cluster_slots(value, &default_host)?;
    data.sort_by(|a, b| a.start.cmp(&b.start));

    Ok(ClusterRouting { data })
  }

  /// Create a new routing table from the result of the `CLUSTER SLOTS` command.
  ///
  /// The `default_host` value refers to the server that provided the response.
  pub fn from_resp2_cluster_slots<S: Into<Str>>(value: Resp2Frame, default_host: S) -> Result<Self, RedisError> {
    let default_host = default_host.into();
    let mut data = parse_cluster_slots(value, &default_host)?;
    data.sort_by(|a, b| a.start.cmp(&b.start));

    Ok(ClusterRouting { data })
  }

  /// Read a set of unique hash slots that each map to a different primary/main node in the cluster.
  pub fn unique_hash_slots(&self) -> Vec<u16> {
    let mut out = BTreeMap::new();

    for slot in self.data.iter() {
      out.insert(&slot.primary, slot.start);
    }

    out.into_iter().map(|(_, v)| v).collect()
  }

  /// Read the set of unique primary nodes in the cluster.
  pub fn unique_primary_nodes(&self) -> Vec<Server> {
    let mut out = BTreeSet::new();

    for slot in self.data.iter() {
      out.insert(slot.primary.clone());
    }

    out.into_iter().collect()
  }

  /// Rebuild the cache in place with the output of a `CLUSTER SLOTS` command.
  pub(crate) fn rebuild(
    &mut self,
    inner: &Arc<RedisClientInner>,
    cluster_slots: RedisValue,
    default_host: &Str,
  ) -> Result<(), RedisError> {
    self.data = cluster::parse_cluster_slots(cluster_slots, default_host)?;
    self.data.sort_by(|a, b| a.start.cmp(&b.start));

    cluster::modify_cluster_slot_hostnames(inner, &mut self.data, default_host);
    Ok(())
  }

  /// Calculate the cluster hash slot for the provided key.
  pub fn hash_key(key: &[u8]) -> u16 {
    redis_protocol::redis_keyslot(key)
  }

  /// Find the primary server that owns the provided hash slot.
  pub fn get_server(&self, slot: u16) -> Option<&Server> {
    if self.data.is_empty() {
      return None;
    }

    protocol_utils::binary_search(&self.data, slot).map(|idx| &self.data[idx].primary)
  }

  /// Read the replicas associated with the provided primary node based on the cached CLUSTER SLOTS response.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn replicas(&self, primary: &Server) -> Vec<Server> {
    self
      .data
      .iter()
      .fold(BTreeSet::new(), |mut replicas, slot| {
        if slot.primary == *primary {
          replicas.extend(slot.replicas.clone());
        }

        replicas
      })
      .into_iter()
      .collect()
  }

  /// Read the number of hash slot ranges in the cluster.
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Read the hash slot ranges in the cluster.
  pub fn slots(&self) -> &[SlotRange] {
    &self.data
  }

  /// Read a random primary node hash slot range from the cluster cache.
  pub fn random_slot(&self) -> Option<&SlotRange> {
    if !self.data.is_empty() {
      let idx = rand::thread_rng().gen_range(0 .. self.data.len());
      Some(&self.data[idx])
    } else {
      None
    }
  }

  /// Read a random primary node from the cluster cache.
  pub fn random_node(&self) -> Option<&Server> {
    self.random_slot().map(|slot| &slot.primary)
  }

  /// Print the contents of the routing table as a human-readable map.
  pub fn pretty(&self) -> BTreeMap<Server, (Vec<(u16, u16)>, BTreeSet<Server>)> {
    let mut out = BTreeMap::new();
    for slot_range in self.data.iter() {
      let entry = out
        .entry(slot_range.primary.clone())
        .or_insert((Vec::new(), BTreeSet::new()));
      entry.0.push((slot_range.start, slot_range.end));
      #[cfg(feature = "replicas")]
      entry.1.extend(slot_range.replicas.iter().cloned());
    }

    out
  }
}
