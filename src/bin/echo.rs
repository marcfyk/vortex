use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    error,
    io::{self, Write},
};
use vortex::{Message, Node};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
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

pub struct EchoNode {
    msg_id_counter: usize,
}

impl vortex::Node<Payload> for EchoNode {
    fn handle_message(
        &mut self,
        writer: &mut impl Write,
        message: Message<Payload>,
    ) -> Result<(), Box<dyn error::Error>> {
        Self::update_msg_id(&mut self.msg_id_counter);
        match message.body {
            Payload::Echo { msg_id, echo } => {
                let m = Message {
                    src: message.dest,
                    dest: message.src,
                    body: Payload::EchoOk {
                        msg_id: self.msg_id_counter,
                        in_reply_to: msg_id,
                        echo,
                    },
                };
                m.write(writer)?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let _ = vortex::init()?;
    let mut node = EchoNode { msg_id_counter: 0 };

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lines() {
        let line = line?;
        let message: Message<Payload> = serde_json::from_str(&line)?;
        node.handle_message(&mut stdout, message)?;
    }

    Ok(())
}
