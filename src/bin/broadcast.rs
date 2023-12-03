use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    error, io,
    sync::mpsc,
    thread,
    time::Duration,
};
use vortex::{Message, Node};

struct UnAckedMessage {
    node: String,
    message: usize,
}

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
    ResendUnAcked,
}

struct BroadcastNode {
    id: String,
    msg_id_counter: usize,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    message_buffer: HashMap<usize, UnAckedMessage>,
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
                        .map(|n| {
                            Self::update_msg_id(&mut self.msg_id_counter);
                            let src = self.id.clone();
                            let dest = n.to_string();
                            let msg_id = self.msg_id_counter;
                            let body = Payload::Broadcast { msg_id, message };
                            self.message_buffer.insert(
                                msg_id,
                                UnAckedMessage {
                                    node: n.to_string(),
                                    message,
                                },
                            );
                            Message { src, dest, body }
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
            Payload::BroadcastOk { in_reply_to, .. } => {
                self.message_buffer.remove(&in_reply_to);
            }
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
            Payload::ResendUnAcked => {
                let mut updated_message_buffer = HashMap::new();
                for UnAckedMessage { node, message } in self.message_buffer.values() {
                    Self::update_msg_id(&mut self.msg_id_counter);
                    let src = self.id.to_string();
                    let dest = node.to_string();
                    let msg_id = self.msg_id_counter;
                    let message = *message;
                    let body = Payload::Broadcast { msg_id, message };
                    updated_message_buffer.insert(
                        msg_id,
                        UnAckedMessage {
                            node: dest.to_string(),
                            message,
                        },
                    );
                    let m = Message { src, dest, body };
                    m.write(writer)?;
                }
                self.message_buffer = updated_message_buffer;
            }
        };
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let init = vortex::init()?;
    let mut node = BroadcastNode {
        id: init.body.node_id.clone(),
        msg_id_counter: 0,
        messages: HashSet::new(),
        neighbors: Vec::new(),
        message_buffer: HashMap::new(),
    };

    let (message_tx, message_rx) = mpsc::channel();
    let resend_tx = message_tx.clone();
    let message_handler = thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lines() {
            let line = line.expect("no line read from stdin");
            let message: Message<Payload> =
                serde_json::from_str(&line).expect("could not parse message");
            message_tx.send(message).expect("could not send message");
        }
    });

    let (terminate_resend_tx, terminate_resend_rx) = mpsc::channel();
    let resend_handler = thread::spawn(move || {
        let interval = Duration::from_millis(1000);
        loop {
            thread::sleep(interval);
            if let Ok(()) = terminate_resend_rx.try_recv() {
                break;
            }
            let message: Message<Payload> = Message {
                src: init.body.node_id.clone(),
                dest: init.body.node_id.clone(),
                body: Payload::ResendUnAcked {},
            };
            resend_tx.send(message).expect("could not send message");
        }
    });

    let mut stdout = io::stdout();
    for message in message_rx {
        node.handle_message(&mut stdout, message)?;
    }

    message_handler
        .join()
        .expect("could not join message handler");

    terminate_resend_tx
        .send(())
        .expect("could not send terminating message to thread");
    resend_handler.join().expect("could not resend handler");

    Ok(())
}
