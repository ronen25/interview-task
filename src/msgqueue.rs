use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, Arc};
use std::error;
use std::thread;

use crate::message::Message;

use chrono::Local;
use std::borrow::BorrowMut;

pub struct QueueManager {
    queues: HashMap<String, MessageQueue>,
}

pub struct LockableMessageQueue {
    pub queue: Mutex<VecDeque<Message>>
}

pub struct MessageQueue {
    internal: LockableMessageQueue
}

impl QueueManager {
    pub fn new() -> QueueManager {
        // Would've made it nicer but didn't have any time
        QueueManager {
            queues: HashMap::new()
        }
    }

    pub fn create_queue(&mut self, name: &str) -> Result<(), Box<dyn error::Error + '_>>{
        self.queues.insert(name.to_owned(), MessageQueue::new());

        Ok(())
    }

    pub fn queue_exists(&self, name: &str) -> Result<bool, Box<dyn error::Error + '_>> {
        Ok(self.queues.contains_key(name))
    }

    pub fn post_message(&mut self, queue_name: &str, msg: Message) -> Result<(), Box<dyn error::Error + '_>> {
        let mut queue = self.queues.get(queue_name).unwrap();
        queue.post(msg)
    }

    pub fn get_message(&mut self, queue_name: &str) -> Result<Option<Message>,
        Box<dyn error::Error + '_>> {
        let mut queue = self.queues.get(queue_name).unwrap();
        queue.get()
    }
}

impl LockableMessageQueue {
    pub fn new() -> LockableMessageQueue {
        LockableMessageQueue {
            queue: Mutex::new(VecDeque::new())
        }
    }

    pub fn post(&mut self, msg: Message) -> Result<(), Box<dyn error::Error + '_>>{
        let mut queue = self.queue.lock()?;
        queue.push_back(msg);

        Ok(())
    }

    pub fn get(&mut self, timeout: Option<i64>) -> Result<Option<Message>, Box<dyn error::Error + '_>> {
        let wait_max = timeout.unwrap_or(1000); // 10 seconds in ms
        let mut timed_out = false;

        // Wait, if needed, until there's something in the queue
        let mut queue = self.queue.lock()?;
        let start_time = Local::now();
        while queue.is_empty() {
            // Check if timeout has passed
            let diff = Local::now() - start_time;
            if diff.num_milliseconds() >= wait_max {
                timed_out = true;
                break;
            }

            // Keep waiting (but yield to avoid busy wait)
            thread::yield_now();
        }

        // If nothing found, return none
        if !timed_out {
            return Ok(None);
        }

        Ok(queue.pop_back())
    }
}

impl MessageQueue {
    pub fn new() -> MessageQueue {
        MessageQueue {
            internal: LockableMessageQueue::new()
        }
    }

    pub fn post(&mut self, msg: Message) -> Result<(), Box<dyn error::Error + '_>> {
        self.internal.post(msg)
    }

    pub fn get(&mut self, timeout: Option<i64>) -> Result<Option<Message>, Box<dyn error::Error + '_>> {
        self.internal.get(timeout)
    }
}
