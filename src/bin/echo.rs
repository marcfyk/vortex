use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead};
use vortex::{Init, Message, Node, StateMachine};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        msg_id: usize,
        echo: String,
    },
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
}

struct EchoNode {
    msg_id_counter: usize,
}

impl EchoNode {
    fn new() -> Self {
        let msg_id_counter = 0;
        Self { msg_id_counter }
    }
}

impl StateMachine<Payload> for EchoNode {
    fn apply(&mut self, messages: Vec<Message<Payload>>) -> Result<Vec<Message<Payload>>> {
        let mut responses = Vec::new();
        for Message { src, dest, body } in messages {
            if let Payload::Echo { msg_id, echo } = body {
                self.msg_id_counter += 1;
                responses.push(Message {
                    src: dest,
                    dest: src,
                    body: Payload::EchoOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                        echo,
                    },
                });
            }
        }
        Ok(responses)
    }
}

fn main() -> Result<()> {
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let init: Message<Init> = Message::from_reader(&mut stdin)?;
    let (mut node, resp) = Node::init(init, Box::new(EchoNode::new()));
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
