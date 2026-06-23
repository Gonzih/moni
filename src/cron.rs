use std::str::FromStr;

use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::queue::{NamespaceQueue, QueuedPrompt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronTaskStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronTask {
    pub id: String,
    pub namespace: String,
    pub repo_url: String,
    pub schedule: String,
    pub message: String,
    pub status: CronTaskStatus,
    pub fire_count: u64,
    pub last_run: Option<DateTime<Utc>>,
    pub compact_every: Option<u64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CronTask {
    pub fn new(
        namespace: impl Into<String>,
        repo_url: impl Into<String>,
        schedule: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: format!("cron-{}", Uuid::new_v4()),
            namespace: namespace.into(),
            repo_url: repo_url.into(),
            schedule: schedule.into(),
            message: message.into(),
            status: CronTaskStatus::Active,
            fire_count: 0,
            last_run: None,
            compact_every: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn should_fire_at(&self, now: DateTime<Utc>) -> anyhow::Result<bool> {
        if self.status != CronTaskStatus::Active {
            return Ok(false);
        }

        let schedule = parse_schedule(&self.schedule)?;
        let after = self.last_run.unwrap_or(self.created_at);
        let Some(next) = schedule.after(&after).next() else {
            return Ok(false);
        };

        Ok(next <= now)
    }

    pub fn needs_compact_before_next_fire(&self) -> bool {
        match self.compact_every {
            Some(n) if n > 0 => self.fire_count > 0 && self.fire_count % n == 0,
            _ => false,
        }
    }
}

#[derive(Debug, Default)]
pub struct CronEngine {
    tasks: Vec<CronTask>,
}

impl CronEngine {
    pub fn new(tasks: Vec<CronTask>) -> Self {
        Self { tasks }
    }

    pub fn tasks(&self) -> &[CronTask] {
        &self.tasks
    }

    pub async fn tick<Q: NamespaceQueue>(
        &mut self,
        queue: &Q,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Vec<String>> {
        let mut fired = Vec::new();

        for task in &mut self.tasks {
            if !task.should_fire_at(now)? {
                continue;
            }

            if task.needs_compact_before_next_fire() {
                queue
                    .enqueue(QueuedPrompt::new(
                        task.namespace.clone(),
                        Some(task.repo_url.clone()),
                        "/compact",
                        format!("cron:{}:compact", task.id),
                    ))
                    .await?;
            }

            queue
                .enqueue(QueuedPrompt::new(
                    task.namespace.clone(),
                    Some(task.repo_url.clone()),
                    task.message.clone(),
                    format!("cron:{}", task.id),
                ))
                .await?;

            task.fire_count += 1;
            task.last_run = Some(now);
            task.updated_at = now;
            fired.push(task.id.clone());
        }

        Ok(fired)
    }
}

fn parse_schedule(schedule: &str) -> anyhow::Result<Schedule> {
    let fields = schedule.split_whitespace().count();
    let normalized = if fields == 5 {
        format!("0 {schedule}")
    } else {
        schedule.to_string()
    };
    Ok(Schedule::from_str(&normalized)?)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::queue::InMemoryNamespaceQueue;

    #[tokio::test]
    async fn cron_fire_enqueues_prompt_into_namespace_queue() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "https://example.com/moni.git", "* * * * *", "run");
        task.created_at = created_at;

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![task]);

        let fired = engine.tick(&queue, now).await.unwrap();

        assert_eq!(fired.len(), 1);
        let drained = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].body, "run");
        assert_eq!(drained[0].source, format!("cron:{}", fired[0]));
    }

    #[tokio::test]
    async fn cron_compacts_before_scheduled_message_when_due() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "https://example.com/moni.git", "* * * * *", "run");
        task.created_at = created_at;
        task.fire_count = 5;
        task.compact_every = Some(5);

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![task]);

        engine.tick(&queue, now).await.unwrap();

        let drained = queue.drain_namespace("moni").await.unwrap();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].body, "/compact");
        assert_eq!(drained[1].body, "run");
    }
}
