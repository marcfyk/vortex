use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error,
    io::{self, BufRead},
};
use vortex::{Message, MessageError, Node, Payload, StateMachine};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Data {
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

impl StateMachine<Data> for BroadcastNode {
    fn apply(
        &mut self,
        messages: Vec<Message<Data>>,
    ) -> Result<Vec<Message<Data>>, Box<dyn error::Error>> {
        let mut responses = Vec::new();
        for message in messages {
            let Message { src, dest, body } = message;
            match body {
                Payload::Custom(Data::Broadcast { msg_id, message }) => {
                    if !self.messages.contains(&message) {
                        self.neighbors
                            .iter()
                            .filter(|&n| *n != src && *n != dest)
                            .map(|n| {
                                self.msg_id_counter += 1;
                                let src = self.id.to_string();
                                let dest = n.to_string();
                                let msg_id = self.msg_id_counter;
                                let body = Payload::Custom(Data::Broadcast { msg_id, message });
                                Message { src, dest, body }
                            })
                            .for_each(|m| responses.push(m));
                    }
                    self.msg_id_counter += 1;
                    self.messages.insert(message);
                    responses.push(Message {
                        src: dest,
                        dest: src,
                        body: Payload::Custom(Data::BroadcastOk {
                            msg_id: self.msg_id_counter,
                            in_reply_to: msg_id,
                        }),
                    });
                }
                Payload::Custom(Data::Read { msg_id }) => {
                    self.msg_id_counter += 1;
                    responses.push(Message {
                        src: dest,
                        dest: src,
                        body: Payload::Custom(Data::ReadOk {
                            msg_id: self.msg_id_counter,
                            in_reply_to: msg_id,
                            messages: self.messages.iter().copied().collect(),
                        }),
                    });
                }
                Payload::Custom(Data::Topology { msg_id, topology }) => {
                    self.msg_id_counter += 1;
                    self.neighbors = topology.get(&self.id).unwrap_or(&vec![]).clone();
                    responses.push(Message {
                        src: dest,
                        dest: src,
                        body: Payload::Custom(Data::TopologyOk {
                            msg_id: self.msg_id_counter,
                            in_reply_to: msg_id,
                        }),
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

    let init: Message<Data> = Message::from_reader(&mut stdin)?;
    let id = match &init.body {
        Payload::Init { node_id, .. } => Ok(node_id.to_string()),
        _ => Err(MessageError::Invalid),
    }?;
    let (mut node, resp) = Node::init(init, Box::new(BroadcastNode::new(&id)))?;
    resp.write(&mut stdout)?;

    for line in stdin.lines() {
        let message: Message<Data> = Message::from_str(&line?)?;
        let responses = node.recv_messages(vec![message])?;
        for res in responses {
            res.write(&mut stdout)?;
        }
    }
    Ok(())
}
