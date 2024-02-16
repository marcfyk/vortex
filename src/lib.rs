use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json;
use std::io::{BufRead, Write};

/// The RPC messages exchanged between Maelstrom's clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<T> {
    /// The node the message comes from.
    pub src: String,
    /// The node this message is to.
    pub dest: String,
    /// The payload of the message.
    pub body: T,
}

impl<T> Message<T>
where
    T: DeserializeOwned,
{
    /// This is used to deserialize a message from a buffered reader.
    pub fn from_reader(reader: &mut impl BufRead) -> Result<Self> {
        let mut message = String::new();
        reader.read_line(&mut message)?;
        let message = serde_json::from_str(&message)
            .expect(format!("message deserialization error: {:?}", message).as_str());
        Ok(message)
    }

    /// This is used to deserialize a message from a string.
    pub fn from_str(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

impl<T> Message<T>
where
    T: Serialize,
{
    /// This is used to serialize a message to a writer with a trailing newline
    /// as specified by Maelstrom's protocol.
    pub fn write(&self, writer: &mut impl Write) -> Result<()> {
        serde_json::to_writer(&mut *writer, self)?;
        writer.write_all(b"\n")?;
        Ok(())
    }
}

/// The init message Maelstrom sends to each node at the start of a test.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "init")]
pub struct Init {
    /// The unique integer ID from the sender.
    msg_id: usize,
    /// The ID of the node that receives this message.
    pub node_id: String,
    /// All nodes in the cluster including the node receiving the message.
    node_ids: Vec<String>,
}

/// The node's response to `Message<Init>` message.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "init_ok")]
pub struct InitOk {
    /// The msg_id of the request.
    in_reply_to: usize,
}

/// The error message that can be used to respond to a Maelstrom RPC request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename = "error")]
pub struct Error {
    /// The msg_id of the request.
    in_reply_to: usize,
    /// The error code, 0-999 are reserved for Maelstrom, 1000+ are for custom error codes.
    code: usize,
    /// The optional message explaining the error.
    text: Option<String>,
}

/// This represents the Maelstrom node.
pub struct Node<T> {
    /// The ID of the node.
    id: String,
    /// The nodes in the cluster including itself.
    peers: Vec<String>,
    /// The state of the node, which is polymorphic based on the application.
    /// This should contain the business state of the application.
    state_machine: Box<dyn StateMachine<T>>,
}

impl<T> Node<T> {
    /// This initializes the server based on an init message,
    /// returning the node and the response to the init message.
    pub fn init(
        message: Message<Init>,
        state_machine: Box<dyn StateMachine<T>>,
    ) -> (Self, Message<InitOk>) {
        let Init {
            msg_id,
            node_id,
            node_ids,
        } = message.body;
        let node = Self {
            id: node_id,
            peers: node_ids,
            state_machine,
        };
        let resp = Message {
            src: message.dest,
            dest: message.src,
            body: InitOk {
                in_reply_to: msg_id,
            },
        };
        (node, resp)
    }

    pub fn recv_messages(&mut self, messages: Vec<Message<T>>) -> Result<Vec<Message<T>>> {
        self.state_machine.apply(messages)
    }
}

/// This is a trait for applications to implement how messages should affect the node's state.
/// This should be implemented based on the application's specific needs.
pub trait StateMachine<T> {
    /// This specifies how the state machine should be affected based on the sequence of messages,
    /// and returns a sequence of responses.
    fn apply(&mut self, messages: Vec<Message<T>>) -> Result<Vec<Message<T>>>;
}
