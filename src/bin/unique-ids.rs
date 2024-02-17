use serde::{Deserialize, Serialize};
use std::{
    error,
    io::{self, BufRead},
};
use vortex::{Message, MessageError, Node, Payload, StateMachine};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Data {
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

impl StateMachine<Data> for UniqueIdsNode {
    fn apply(
        &mut self,
        messages: Vec<Message<Data>>,
    ) -> Result<Vec<Message<Data>>, Box<dyn error::Error>> {
        let mut responses = Vec::new();
        for Message { src, dest, body } in messages {
            if let Payload::Custom(Data::Generate { msg_id }) = body {
                self.msg_id_counter += 1;
                responses.push(Message {
                    src: dest,
                    dest: src,
                    body: Payload::Custom(Data::GenerateOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                        id: format!("{}/{}", self.id, self.msg_id_counter),
                    }),
                });
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
    let (mut node, resp) = Node::init(init, Box::new(UniqueIdsNode::new(&id)))?;
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
