use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    error,
    io::{self, BufRead},
};
use vortex::{Init, Message, Node, StateMachine};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate {
        msg_id: usize,
    },
    GenerateOk {
        msg_id: usize,
        in_reply_to: usize,
        id: String,
    },
}

struct UniqueIdsNode {
    id: String,
    msg_id_counter: usize,
}

impl UniqueIdsNode {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            msg_id_counter: 0,
        }
    }
}

impl StateMachine<Payload> for UniqueIdsNode {
    fn apply(&mut self, messages: Vec<Message<Payload>>) -> Result<Vec<Message<Payload>>> {
        let mut responses = Vec::new();
        for Message { src, dest, body } in messages {
            if let Payload::Generate { msg_id } = body {
                self.msg_id_counter += 1;
                responses.push(Message {
                    src: dest,
                    dest: src,
                    body: Payload::GenerateOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                        id: format!("{}/{}", self.id, self.msg_id_counter),
                    },
                });
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
    let (mut node, resp) = Node::init(init, Box::new(UniqueIdsNode::new(&id)));
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
