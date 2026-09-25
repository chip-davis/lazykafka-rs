use serde::{Deserialize, Serialize};
use strum::Display;

use crate::kafka;

#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize)]
pub enum Action {
    Tick,
    Render,
    Resize(u16, u16),
    Suspend,
    Resume,
    Quit,
    ClearScreen,
    Error(String),
    Help,
    TopicsLoaded(Vec<crate::kafka::Topic>),
    TopicSelected(String),
    PartitionsLoaded(String, Vec<kafka::Partition>),
    PartitionSelected(String, Vec<i32>),
    RecordReceived(String, kafka::Record),
    RecordSelected(Option<kafka::Record>),
    LoadNextRecords(String, Vec<i32>, Vec<(i32, i64)>),
    HistoricalRecordsLoaded(String, Vec<i32>, Vec<kafka::Record>, bool),
    TailSelected(String, Vec<i32>),
    TailStopped,
    PageFailed(String, Vec<i32>),
    ToggleDownload,
}
