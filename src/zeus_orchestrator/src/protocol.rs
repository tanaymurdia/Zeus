use zeus_node::cell::Cell;

pub const MSG_LOAD_REPORT: u8 = 0xD0;
pub const MSG_REQUEST_SPLIT: u8 = 0xD1;
pub const MSG_REQUEST_MERGE: u8 = 0xD2;
pub const MSG_CELL_ASSIGN: u8 = 0xD3;
pub const MSG_NODE_SHUTDOWN: u8 = 0xD4;
pub const MSG_TOPOLOGY_UPDATE: u8 = 0xD5;

#[derive(Debug, Clone)]
pub struct LoadReport {
    pub node_id: u64,
    pub entity_count: u32,
    pub cpu_pct: u8,
    pub cell: Cell,
}

#[derive(Debug, Clone)]
pub struct RequestSplit {
    pub node_id: u64,
    pub cell: Cell,
}

#[derive(Debug, Clone)]
pub struct RequestMerge {
    pub node_id: u64,
    pub cell: Cell,
}

#[derive(Debug, Clone)]
pub struct NeighborInfo {
    pub addr: std::net::SocketAddr,
    pub cell: Cell,
}

#[derive(Debug, Clone)]
pub struct CellAssign {
    pub cell: Cell,
    pub neighbors: Vec<NeighborInfo>,
}

#[derive(Debug, Clone)]
pub struct NodeShutdown {
    pub reason: u8,
    pub target_addr: Option<std::net::SocketAddr>,
}

impl LoadReport {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(38);
        buf.push(MSG_LOAD_REPORT);
        buf.extend_from_slice(&self.node_id.to_le_bytes());
        buf.extend_from_slice(&self.entity_count.to_le_bytes());
        buf.push(self.cpu_pct);
        buf.extend_from_slice(&self.cell.serialize());
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 38 || data[0] != MSG_LOAD_REPORT { return None; }
        let node_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let entity_count = u32::from_le_bytes(data[9..13].try_into().ok()?);
        let cpu_pct = data[13];
        let cell = Cell::deserialize(&data[14..38])?;
        Some(Self { node_id, entity_count, cpu_pct, cell })
    }
}

impl RequestSplit {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(33);
        buf.push(MSG_REQUEST_SPLIT);
        buf.extend_from_slice(&self.node_id.to_le_bytes());
        buf.extend_from_slice(&self.cell.serialize());
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 33 || data[0] != MSG_REQUEST_SPLIT { return None; }
        let node_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let cell = Cell::deserialize(&data[9..33])?;
        Some(Self { node_id, cell })
    }
}

impl RequestMerge {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(33);
        buf.push(MSG_REQUEST_MERGE);
        buf.extend_from_slice(&self.node_id.to_le_bytes());
        buf.extend_from_slice(&self.cell.serialize());
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 33 || data[0] != MSG_REQUEST_MERGE { return None; }
        let node_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let cell = Cell::deserialize(&data[9..33])?;
        Some(Self { node_id, cell })
    }
}

fn encode_socket_addr(addr: &std::net::SocketAddr) -> [u8; 18] {
    let mut buf = [0u8; 18];
    match addr {
        std::net::SocketAddr::V4(v4) => {
            buf[0] = 4;
            buf[1] = 0;
            buf[2..6].copy_from_slice(&v4.ip().octets());
            buf[6..8].copy_from_slice(&v4.port().to_le_bytes());
        }
        std::net::SocketAddr::V6(v6) => {
            buf[0] = 6;
            buf[1] = 0;
            buf[2..18].copy_from_slice(&v6.ip().octets());
        }
    }
    buf
}

fn decode_socket_addr(data: &[u8]) -> Option<std::net::SocketAddr> {
    if data.len() < 18 { return None; }
    match data[0] {
        4 => {
            let ip = std::net::Ipv4Addr::new(data[2], data[3], data[4], data[5]);
            let port = u16::from_le_bytes([data[6], data[7]]);
            Some(std::net::SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        }
        6 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&data[2..18]);
            let ip = std::net::Ipv6Addr::from(octets);
            Some(std::net::SocketAddr::V6(std::net::SocketAddrV6::new(ip, 0, 0, 0)))
        }
        _ => None,
    }
}

impl CellAssign {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(25 + 1 + self.neighbors.len() * 42);
        buf.push(MSG_CELL_ASSIGN);
        buf.extend_from_slice(&self.cell.serialize());
        buf.push(self.neighbors.len() as u8);
        for n in &self.neighbors {
            buf.extend_from_slice(&encode_socket_addr(&n.addr));
            buf.extend_from_slice(&n.cell.serialize());
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 26 || data[0] != MSG_CELL_ASSIGN { return None; }
        let cell = Cell::deserialize(&data[1..25])?;
        let neighbor_count = data[25] as usize;
        let mut offset = 26;
        let mut neighbors = Vec::with_capacity(neighbor_count);
        for _ in 0..neighbor_count {
            if offset + 42 > data.len() { return None; }
            let addr = decode_socket_addr(&data[offset..offset + 18])?;
            let ncell = Cell::deserialize(&data[offset + 18..offset + 42])?;
            neighbors.push(NeighborInfo { addr, cell: ncell });
            offset += 42;
        }
        Some(Self { cell, neighbors })
    }
}

impl NodeShutdown {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20);
        buf.push(MSG_NODE_SHUTDOWN);
        buf.push(self.reason);
        if let Some(addr) = &self.target_addr {
            buf.extend_from_slice(&encode_socket_addr(addr));
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 2 || data[0] != MSG_NODE_SHUTDOWN { return None; }
        let reason = data[1];
        let target_addr = if data.len() >= 20 {
            decode_socket_addr(&data[2..20])
        } else {
            None
        };
        Some(Self { reason, target_addr })
    }
}

pub fn encode_topology_update(octree_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + octree_bytes.len());
    buf.push(MSG_TOPOLOGY_UPDATE);
    buf.extend_from_slice(octree_bytes);
    buf
}

pub fn decode_topology_update(data: &[u8]) -> Option<&[u8]> {
    if data.is_empty() || data[0] != MSG_TOPOLOGY_UPDATE { return None; }
    Some(&data[1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_report_roundtrip() {
        let lr = LoadReport {
            node_id: 42,
            entity_count: 1000,
            cpu_pct: 75,
            cell: Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0),
        };
        let encoded = lr.encode();
        let decoded = LoadReport::decode(&encoded).unwrap();
        assert_eq!(decoded.node_id, 42);
        assert_eq!(decoded.entity_count, 1000);
        assert_eq!(decoded.cpu_pct, 75);
        assert_eq!(decoded.cell, lr.cell);
    }

    #[test]
    fn test_request_split_roundtrip() {
        let rs = RequestSplit {
            node_id: 7,
            cell: Cell::new(1.0, 5.0, 2.0, 6.0, 3.0, 7.0),
        };
        let encoded = rs.encode();
        let decoded = RequestSplit::decode(&encoded).unwrap();
        assert_eq!(decoded.node_id, 7);
        assert_eq!(decoded.cell, rs.cell);
    }

    #[test]
    fn test_request_merge_roundtrip() {
        let rm = RequestMerge {
            node_id: 99,
            cell: Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0),
        };
        let encoded = rm.encode();
        let decoded = RequestMerge::decode(&encoded).unwrap();
        assert_eq!(decoded.node_id, 99);
        assert_eq!(decoded.cell, rm.cell);
    }

    #[test]
    fn test_cell_assign_roundtrip() {
        let ca = CellAssign {
            cell: Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0),
            neighbors: vec![
                NeighborInfo {
                    addr: "127.0.0.1:5000".parse().unwrap(),
                    cell: Cell::new(10.0, 20.0, 0.0, 10.0, 0.0, 10.0),
                },
                NeighborInfo {
                    addr: "127.0.0.1:5001".parse().unwrap(),
                    cell: Cell::new(0.0, 10.0, 10.0, 20.0, 0.0, 10.0),
                },
            ],
        };
        let encoded = ca.encode();
        let decoded = CellAssign::decode(&encoded).unwrap();
        assert_eq!(decoded.cell, ca.cell);
        assert_eq!(decoded.neighbors.len(), 2);
        assert_eq!(decoded.neighbors[0].addr, "127.0.0.1:5000".parse::<std::net::SocketAddr>().unwrap());
        assert_eq!(decoded.neighbors[1].cell, Cell::new(0.0, 10.0, 10.0, 20.0, 0.0, 10.0));
    }

    #[test]
    fn test_node_shutdown_roundtrip() {
        let ns = NodeShutdown { reason: 1, target_addr: Some("127.0.0.1:6000".parse().unwrap()) };
        let encoded = ns.encode();
        let decoded = NodeShutdown::decode(&encoded).unwrap();
        assert_eq!(decoded.reason, 1);
        assert_eq!(decoded.target_addr.unwrap(), "127.0.0.1:6000".parse::<std::net::SocketAddr>().unwrap());
    }

    #[test]
    fn test_node_shutdown_no_target() {
        let ns = NodeShutdown { reason: 0, target_addr: None };
        let encoded = ns.encode();
        let decoded = NodeShutdown::decode(&encoded).unwrap();
        assert_eq!(decoded.reason, 0);
        assert!(decoded.target_addr.is_none());
    }

    #[test]
    fn test_topology_update_roundtrip() {
        let tree_data = vec![1, 2, 3, 4, 5];
        let encoded = encode_topology_update(&tree_data);
        let decoded = decode_topology_update(&encoded).unwrap();
        assert_eq!(decoded, &[1, 2, 3, 4, 5]);
    }
}
