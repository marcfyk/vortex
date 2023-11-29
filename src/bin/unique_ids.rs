use serde::{Deserialize, Serialize};
use serde_json;
use std::{error, io};
use vortex::{self, Message, Node};

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

impl Node<Payload> for UniqueIdsNode {
    fn handle_message(
        &mut self,
        writer: &mut impl io::Write,
        message: vortex::Message<Payload>,
    ) -> Result<(), Box<dyn error::Error>> {
        match message.body {
            Payload::Generate { msg_id } => {
                Self::update_msg_id(&mut self.msg_id_counter);
                let id = format!("{}/{}", self.id, self.msg_id_counter);
                let m = Message {
                    src: message.dest,
                    dest: message.src,
                    body: Payload::GenerateOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                        id,
                    },
                };
                m.write(writer)?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let init = vortex::init()?;
    let mut node = UniqueIdsNode {
        id: init.body.node_id,
        msg_id_counter: 0,
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
