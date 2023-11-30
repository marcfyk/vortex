use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    error, io,
};
use vortex::{Message, Node};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        msg_id: usize,
        message: usize,
    },
    BroadcastOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Read {
        msg_id: usize,
    },
    ReadOk {
        msg_id: usize,
        in_reply_to: usize,
        messages: Vec<usize>,
    },
    Topology {
        msg_id: usize,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        msg_id: usize,
        in_reply_to: usize,
    },
}

struct BroadcastNode {
    id: String,
    msg_id_counter: usize,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
}

impl Node<Payload> for BroadcastNode {
    fn handle_message(
        &mut self,
        writer: &mut impl io::Write,
        msg: Message<Payload>,
    ) -> Result<(), Box<dyn error::Error>> {
        match msg.body {
            Payload::Broadcast { msg_id, message } => {
                if !self.messages.contains(&message) {
                    self.neighbors
                        .iter()
                        .filter(|&n| *n != msg.src && *n != msg.dest)
                        .map(|n| Message {
                            src: self.id.clone(),
                            dest: n.to_string(),
                            body: Payload::Broadcast { msg_id, message },
                        })
                        .try_for_each(|m| m.write(writer))?;
                }

                Self::update_msg_id(&mut self.msg_id_counter);
                self.messages.insert(message);
                let m = Message {
                    src: msg.dest,
                    dest: msg.src.clone(),
                    body: Payload::BroadcastOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                    },
                };
                m.write(writer)?;
            }
            Payload::BroadcastOk { .. } => {}
            Payload::Read { msg_id } => {
                Self::update_msg_id(&mut self.msg_id_counter);
                let m = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Payload::ReadOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                        messages: self.messages.iter().copied().collect(),
                    },
                };
                m.write(writer)?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { msg_id, topology } => {
                Self::update_msg_id(&mut self.msg_id_counter);
                self.neighbors = topology.get(&self.id).unwrap_or(&vec![]).clone();
                let m = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Payload::TopologyOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                    },
                };
                m.write(writer)?;
            }
            Payload::TopologyOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let init = vortex::init()?;
    let mut node = BroadcastNode {
        id: init.body.node_id,
        msg_id_counter: 0,
        messages: HashSet::new(),
        neighbors: Vec::new(),
    };

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lines() {
        let line = line?;
        let message: Message<Payload> = serde_json::from_str(&line)?;
        node.handle_message(&mut stdout, message)?;
    }

    Ok(())
}
