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

    pub fn add(&mut self, task: CronTask) {
        self.tasks.push(task);
    }

    pub fn pause(&mut self, id: &str) -> bool {
        self.set_status(id, CronTaskStatus::Paused)
    }

    pub fn resume(&mut self, id: &str) -> bool {
        self.set_status(id, CronTaskStatus::Active)
    }

    pub fn delete(&mut self, id: &str) -> bool {
        let before = self.tasks.len();
        self.tasks.retain(|task| task.id != id);
        self.tasks.len() != before
    }

    fn set_status(&mut self, id: &str, status: CronTaskStatus) -> bool {
        let Some(task) = self.tasks.iter_mut().find(|task| task.id == id) else {
            return false;
        };
        task.status = status;
        task.updated_at = Utc::now();
        true
    }

    pub async fn tick<Q: NamespaceQueue + ?Sized>(
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

    #[test]
    fn cron_task_new_sets_required_fields() {
        let task = CronTask::new("moni", "repo", "*/5 * * * *", "run");
        assert!(task.id.starts_with("cron-"));
        assert_eq!(task.namespace, "moni");
        assert_eq!(task.repo_url, "repo");
        assert_eq!(task.schedule, "*/5 * * * *");
        assert_eq!(task.message, "run");
        assert_eq!(task.status, CronTaskStatus::Active);
        assert_eq!(task.fire_count, 0);
        assert!(task.last_run.is_none());
        assert!(task.compact_every.is_none());
    }

    #[test]
    fn paused_task_should_not_fire() {
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        task.status = CronTaskStatus::Paused;

        assert!(!task.should_fire_at(now).unwrap());
    }

    #[test]
    fn task_should_not_fire_before_next_schedule() {
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 30).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();

        assert!(!task.should_fire_at(now).unwrap());
    }

    #[test]
    fn task_should_fire_at_next_minute() {
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();

        assert!(task.should_fire_at(now).unwrap());
    }

    #[test]
    fn last_run_is_used_for_next_schedule() {
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 30).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        task.last_run = Some(Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap());

        assert!(!task.should_fire_at(now).unwrap());
    }

    #[test]
    fn invalid_schedule_returns_error() {
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "repo", "not cron", "run");
        task.created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();

        assert!(task.should_fire_at(now).is_err());
    }

    #[test]
    fn five_field_cron_is_accepted() {
        assert!(parse_schedule("* * * * *").is_ok());
    }

    #[test]
    fn six_field_cron_is_accepted() {
        assert!(parse_schedule("0 * * * * *").is_ok());
    }

    #[test]
    fn compact_is_false_without_interval() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.fire_count = 10;
        assert!(!task.needs_compact_before_next_fire());
    }

    #[test]
    fn compact_is_false_for_zero_interval() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.fire_count = 10;
        task.compact_every = Some(0);
        assert!(!task.needs_compact_before_next_fire());
    }

    #[test]
    fn compact_is_false_before_first_fire() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.fire_count = 0;
        task.compact_every = Some(5);
        assert!(!task.needs_compact_before_next_fire());
    }

    #[test]
    fn compact_is_true_on_interval_boundary() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.fire_count = 10;
        task.compact_every = Some(5);
        assert!(task.needs_compact_before_next_fire());
    }

    #[test]
    fn compact_is_false_off_interval_boundary() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.fire_count = 11;
        task.compact_every = Some(5);
        assert!(!task.needs_compact_before_next_fire());
    }

    #[tokio::test]
    async fn tick_updates_fire_count_last_run_and_updated_at() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = created_at;

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![task]);

        engine.tick(&queue, now).await.unwrap();

        let task = &engine.tasks()[0];
        assert_eq!(task.fire_count, 1);
        assert_eq!(task.last_run, Some(now));
        assert_eq!(task.updated_at, now);
    }

    #[tokio::test]
    async fn tick_does_not_enqueue_paused_task() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = created_at;
        task.status = CronTaskStatus::Paused;

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![task]);

        let fired = engine.tick(&queue, now).await.unwrap();

        assert!(fired.is_empty());
        assert!(queue.drain_namespace("moni").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_does_not_fire_same_task_twice_for_same_timestamp() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.created_at = created_at;

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![task]);

        assert_eq!(engine.tick(&queue, now).await.unwrap().len(), 1);
        assert!(engine.tick(&queue, now).await.unwrap().is_empty());
        assert_eq!(queue.drain_namespace("moni").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tick_can_fire_multiple_tasks() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut a = CronTask::new("moni", "repo", "* * * * *", "a");
        let mut b = CronTask::new("ops", "repo", "* * * * *", "b");
        a.created_at = created_at;
        b.created_at = created_at;

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![a, b]);

        let fired = engine.tick(&queue, now).await.unwrap();

        assert_eq!(fired.len(), 2);
        assert_eq!(queue.drain_namespace("moni").await.unwrap()[0].body, "a");
        assert_eq!(queue.drain_namespace("ops").await.unwrap()[0].body, "b");
    }

    #[tokio::test]
    async fn tick_keeps_repo_url_on_prompt() {
        let created_at = Utc.with_ymd_and_hms(2026, 6, 23, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 6, 23, 8, 1, 0).unwrap();
        let mut task = CronTask::new("moni", "https://example.com/repo", "* * * * *", "run");
        task.created_at = created_at;

        let queue = InMemoryNamespaceQueue::default();
        let mut engine = CronEngine::new(vec![task]);

        engine.tick(&queue, now).await.unwrap();

        let prompt = queue.drain_namespace("moni").await.unwrap().remove(0);
        assert_eq!(prompt.repo_url.as_deref(), Some("https://example.com/repo"));
    }

    #[test]
    fn cron_task_status_serializes_as_kebab_case() {
        assert_eq!(
            serde_json::to_string(&CronTaskStatus::Active).unwrap(),
            "\"active\""
        );
        assert_eq!(
            serde_json::to_string(&CronTaskStatus::Paused).unwrap(),
            "\"paused\""
        );
    }

    #[test]
    fn cron_task_round_trips_json() {
        let task = CronTask::new("moni", "repo", "* * * * *", "run");
        let encoded = serde_json::to_string(&task).unwrap();
        let decoded: CronTask = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, task);
    }

    #[test]
    fn add_appends_task() {
        let mut engine = CronEngine::default();
        engine.add(CronTask::new("moni", "repo", "* * * * *", "run"));
        assert_eq!(engine.tasks().len(), 1);
    }

    #[test]
    fn pause_existing_task() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.id = "c1".to_string();
        let mut engine = CronEngine::new(vec![task]);
        assert!(engine.pause("c1"));
        assert_eq!(engine.tasks()[0].status, CronTaskStatus::Paused);
    }

    #[test]
    fn pause_missing_task_returns_false() {
        let mut engine = CronEngine::default();
        assert!(!engine.pause("missing"));
    }

    #[test]
    fn resume_existing_task() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.id = "c1".to_string();
        task.status = CronTaskStatus::Paused;
        let mut engine = CronEngine::new(vec![task]);
        assert!(engine.resume("c1"));
        assert_eq!(engine.tasks()[0].status, CronTaskStatus::Active);
    }

    #[test]
    fn delete_existing_task() {
        let mut task = CronTask::new("moni", "repo", "* * * * *", "run");
        task.id = "c1".to_string();
        let mut engine = CronEngine::new(vec![task]);
        assert!(engine.delete("c1"));
        assert!(engine.tasks().is_empty());
    }

    #[test]
    fn delete_missing_task_returns_false() {
        let mut engine = CronEngine::default();
        assert!(!engine.delete("missing"));
    }
}
