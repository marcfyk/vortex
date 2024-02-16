use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error,
    io::{self, BufRead},
};
use vortex::{Init, Message, Node, StateMachine};

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

impl BroadcastNode {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            msg_id_counter: 0,
            messages: HashSet::new(),
            neighbors: Vec::new(),
        }
    }
}

impl StateMachine<Payload> for BroadcastNode {
    fn apply(&mut self, messages: Vec<Message<Payload>>) -> Result<Vec<Message<Payload>>> {
        let mut responses = Vec::new();
        for message in messages {
            let Message { src, dest, body } = message;
            match body {
                Payload::Broadcast { msg_id, message } => {
                    if !self.messages.contains(&message) {
                        self.neighbors
                            .iter()
                            .filter(|&n| *n != src && *n != dest)
                            .map(|n| {
                                self.msg_id_counter += 1;
                                let src = self.id.to_string();
                                let dest = n.to_string();
                                let msg_id = self.msg_id_counter;
                                let body = Payload::Broadcast { msg_id, message };
                                Message { src, dest, body }
                            })
                            .for_each(|m| responses.push(m));
                    }
                    self.msg_id_counter += 1;
                    self.messages.insert(message);
                    responses.push(Message {
                        src: dest,
                        dest: src,
                        body: Payload::BroadcastOk {
                            msg_id: self.msg_id_counter,
                            in_reply_to: msg_id,
                        },
                    });
                }
                Payload::Read { msg_id } => {
                    self.msg_id_counter += 1;
                    responses.push(Message {
                        src: dest,
                        dest: src,
                        body: Payload::ReadOk {
                            msg_id: self.msg_id_counter,
                            in_reply_to: msg_id,
                            messages: self.messages.iter().copied().collect(),
                        },
                    });
                }
                Payload::Topology { msg_id, topology } => {
                    self.msg_id_counter += 1;
                    self.neighbors = topology.get(&self.id).unwrap_or(&vec![]).clone();
                    responses.push(Message {
                        src: dest,
                        dest: src,
                        body: Payload::TopologyOk {
                            msg_id: self.msg_id_counter,
                            in_reply_to: msg_id,
                        },
                    });
                }
                _ => {}
            }
        }
        Ok(responses)
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let init: Message<Init> = Message::from_reader(&mut stdin)?;
    let id = init.body.node_id.to_string();
    let (mut node, resp) = Node::init(init, Box::new(BroadcastNode::new(&id)));
    resp.write(&mut stdout)?;

    for line in stdin.lines() {
        let message: Message<Payload> = Message::from_str(&line?)?;
        let responses = node.recv_messages(vec![message])?;
        for res in responses {
            res.write(&mut stdout)?;
        }
    }
    Ok(())
}
