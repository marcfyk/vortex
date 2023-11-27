use serde::{Deserialize, Serialize};
use std::{
    error,
    io::{self, Write},
};

/// Represents a maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    /// The node this message came from.
    pub src: String,
    /// The node this message is to.
    pub dest: String,
    /// The body contains the payload of the message.
    pub body: T,
}

impl<T> Message<T> {
    /// Writes the message to a writer.
    ///
    /// The message is written to the writer with a trailing newline as specified
    /// by the maelstrom protocol.
    pub fn write(&self, writer: &mut impl Write) -> Result<(), Box<dyn error::Error>>
    where
        T: Serialize,
    {
        serde_json::to_writer(&mut *writer, self)?;
        writer.write_all(b"\n")?;
        Ok(())
    }
}

/// Init message payload sent to each node at the start of a maelstrom test.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "init")]
pub struct Init {
    msg_id: usize,
    /// Node id of the node receiving this message.
    node_id: String,
    /// All node ids in the cluster.
    node_ids: Vec<String>,
}

/// Message payload in response to the init message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "init_ok")]
pub struct InitOk {
    in_reply_to: usize,
}

/// Error message payload that nodes can send.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "error")]
pub struct Error {
    /// the msg id of the request that caused this error.
    in_reply_to: usize,
    /// integer code indicating the type of the error that occurred.
    ///
    /// 0-999 are for maelstrom's use, while 1000 and above are custom codes.
    code: usize,
    /// optional string containing details about the error.
    text: Option<String>,
}

/// A trait for objects that act as maelstrom nodes.
pub trait Node<T> {
    /// Updates the unique msg id for the node.
    ///
    /// The default implementation is usize that increments by 1.
    fn update_msg_id(msg_id: &mut usize) {
        *msg_id += 1;
    }

    /// Handles the behavior of the node when receiving a message,
    /// which can include writing to the writer such as sending message(s) as response(s).
    fn handle_message(
        &mut self,
        writer: &mut impl Write,
        message: Message<T>,
    ) -> Result<(), Box<dyn error::Error>>;
}

/// Runs the initialization for maelstrom by receiving the init message and responds to it.
pub fn init() -> Result<Message<Init>, Box<dyn error::Error>> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    let mut init = String::new();
    stdin.read_line(&mut init)?;
    let init: Message<Init> = serde_json::from_str(&init)?;

    let init_ok = Message {
        src: init.dest.clone(),
        dest: init.src.clone(),
        body: InitOk {
            in_reply_to: init.body.msg_id,
        },
    };
    init_ok.write(&mut stdout)?;
    Ok(init)
}
