use crate::{error::RedisProtocolError, types::REDIS_CLUSTER_SLOTS, utils};
use alloc::collections::{BTreeMap, BTreeSet};
use bytes_utils::Str;
use core::{
  fmt,
  fmt::{write, Formatter},
};
use rand::Rng;

fn binary_search(slots: &[SlotRange], slot: u16) -> Option<usize> {
  if slots.is_empty() || slot > REDIS_CLUSTER_SLOTS {
    return None;
  }

  let (mut low, mut high) = (0, slots.len() - 1);
  while low <= high {
    let mid = (low + high) / 2;

    let curr = match slots.get(mid) {
      Some(slot) => slot,
      None => {
        warn!("Failed to find slot range at index {} for hash slot {}", mid, slot);
        return None;
      },
    };

    if slot < curr.start {
      high = mid - 1;
    } else if slot > curr.end {
      low = mid + 1;
    } else {
      return Some(mid);
    }
  }

  None
}

// TODO
///
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Addr {
  host: String,
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
  pub start:      u16,
  /// The end of the hash slot range.
  pub end:        u16,
  /// The primary server owner.
  pub primary:    Addr,
  /// The internal ID assigned by the server.
  pub primary_id: String,
  /// Replica nodes that own this range.
  pub replicas:   Vec<Addr>,
}

pub struct ClusterChanges {
  pub add:    Vec<Addr>,
  pub remove: Vec<Addr>,
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
  pub fn from_resp3_cluster_slots(value: &str, default_host: &str) -> Result<Self, RedisProtocolError> {
    let default_host = default_host.into();
    let mut data = parse_cluster_slots(value, &default_host)?;
    data.sort_by(|a, b| a.start.cmp(&b.start));

    Ok(ClusterRouting { data })
  }

  ///
  pub fn from_resp3_cluster_shards(value: &str, default_host: &str) -> Result<Self, RedisProtocolError> {
    unimplemented!()
  }

  /// Create a new routing table from the result of the `CLUSTER SLOTS` command.
  ///
  /// The `default_host` value refers to the server that provided the response.
  pub fn from_resp2_cluster_slots(value: &str, default_host: &str) -> Result<Self, RedisProtocolError> {
    let default_host = default_host.into();
    let mut data = parse_cluster_slots(value, &default_host)?;
    data.sort_by(|a, b| a.start.cmp(&b.start));

    Ok(ClusterRouting { data })
  }

  /// TODO
  pub fn from_resp2_cluster_shards(value: &str, default_host: &str) -> Result<Self, RedisProtocolError> {
    unimplemented!()
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
  pub fn unique_primary_nodes(&self) -> Vec<Addr> {
    let mut out = BTreeSet::new();

    for slot in self.data.iter() {
      out.insert(slot.primary.clone());
    }

    out.into_iter().collect()
  }

  // TODO
  pub fn diff(&self, other: &Self) -> ClusterChanges {
    unimplemented!()
  }

  /// Calculate the cluster hash slot for the provided key.
  pub fn hash_key(key: &[u8]) -> u16 {
    utils::redis_keyslot(key)
  }

  /// Find the primary server that owns the provided hash slot.
  pub fn get_server(&self, slot: u16) -> Option<&Addr> {
    if self.data.is_empty() {
      return None;
    }

    binary_search(&self.data, slot).map(|idx| &self.data[idx].primary)
  }

  /// Read the replicas associated with the provided primary node.
  pub fn replicas(&self, primary: &Addr) -> Vec<Addr> {
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
  pub fn random_node(&self) -> Option<&Addr> {
    self.random_slot().map(|slot| &slot.primary)
  }

  /// Print the contents of the routing table as a human-readable map.
  pub fn pretty(&self) -> BTreeMap<Addr, (Vec<(u16, u16)>, BTreeSet<Addr>)> {
    let mut out = BTreeMap::new();
    for slot_range in self.data.iter() {
      let entry = out
        .entry(slot_range.primary.clone())
        .or_insert((Vec::new(), BTreeSet::new()));
      entry.0.push((slot_range.start, slot_range.end));
      entry.1.extend(slot_range.replicas.iter().cloned());
    }

    out
  }
}
