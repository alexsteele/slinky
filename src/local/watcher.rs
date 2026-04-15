use std::path::Path;

use async_trait::async_trait;
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher, recommended_watcher};
use tokio::sync::{mpsc, oneshot};

use crate::services::{Result, SyncError, Watcher, WatcherEvent};

pub struct FsWatcher;

#[async_trait]
impl Watcher for FsWatcher {
    async fn start(
        &self,
        root: &Path,
        tx: mpsc::Sender<WatcherEvent>,
        ready: oneshot::Sender<()>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<()> {
        let root = std::fs::canonicalize(root)?;
        let tx = tx.clone();
        let mut watcher: RecommendedWatcher = recommended_watcher(
            move |result: notify::Result<notify::Event>| match result {
                Ok(event) => {
                    for message in map_notify_event(event) {
                        let _ = tx.blocking_send(message);
                    }
                }
                Err(error) => {
                    eprintln!("[watcher] notify error: {error}");
                    let _ = tx.blocking_send(WatcherEvent::RescanRequested);
                }
            },
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        watcher
            .watch(&root, RecursiveMode::Recursive)
            .map_err(|err| SyncError::InvalidState(err.to_string()))?;
        let _ = ready.send(());

        let _ = (&mut shutdown).await;
        Ok(())
    }
}

fn map_notify_event(event: Event) -> Vec<WatcherEvent> {
    if event.need_rescan() || event.paths.is_empty() {
        return vec![WatcherEvent::RescanRequested];
    }

    match event.kind {
        EventKind::Create(CreateKind::File) => event
            .paths
            .into_iter()
            .map(WatcherEvent::FileChanged)
            .collect(),
        EventKind::Create(CreateKind::Folder) => event
            .paths
            .into_iter()
            .map(WatcherEvent::DirectoryCreated)
            .collect(),
        EventKind::Modify(ModifyKind::Data(_))
        | EventKind::Modify(ModifyKind::Metadata(_))
        | EventKind::Modify(ModifyKind::Other)
        | EventKind::Modify(ModifyKind::Any) => event
            .paths
            .into_iter()
            .map(WatcherEvent::FileChanged)
            .collect(),
        EventKind::Remove(RemoveKind::File) => event
            .paths
            .into_iter()
            .map(WatcherEvent::FileDeleted)
            .collect(),
        EventKind::Remove(RemoveKind::Folder) => event
            .paths
            .into_iter()
            .map(WatcherEvent::DirectoryDeleted)
            .collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) if event.paths.len() == 2 => {
            vec![WatcherEvent::PathMoved {
                from: event.paths[0].clone(),
                to: event.paths[1].clone(),
            }]
        }
        EventKind::Modify(ModifyKind::Name(_))
        | EventKind::Create(CreateKind::Any)
        | EventKind::Create(CreateKind::Other)
        | EventKind::Remove(RemoveKind::Any)
        | EventKind::Remove(RemoveKind::Other)
        | EventKind::Any
        | EventKind::Other
        | EventKind::Access(_) => vec![WatcherEvent::RescanRequested],
    }
}

pub struct NoopWatcher;

#[async_trait]
impl Watcher for NoopWatcher {
    async fn start(
        &self,
        _root: &Path,
        _tx: mpsc::Sender<WatcherEvent>,
        ready: oneshot::Sender<()>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<()> {
        let _ = ready.send(());
        let _ = (&mut shutdown).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use notify::Event;
    use notify::event::{CreateKind, EventKind, ModifyKind, RemoveKind, RenameMode};

    use super::map_notify_event;
    use crate::services::WatcherEvent;

    #[test]
    fn maps_file_write_to_file_changed() {
        let path = PathBuf::from("/tmp/file.txt");
        let events = map_notify_event(
            Event::new(EventKind::Modify(ModifyKind::Data(
                notify::event::DataChange::Content,
            )))
            .add_path(path.clone()),
        );

        assert_eq!(events, vec![WatcherEvent::FileChanged(path)]);
    }

    #[test]
    fn maps_directory_delete() {
        let path = PathBuf::from("/tmp/docs");
        let events = map_notify_event(
            Event::new(EventKind::Remove(RemoveKind::Folder)).add_path(path.clone()),
        );

        assert_eq!(events, vec![WatcherEvent::DirectoryDeleted(path)]);
    }

    #[test]
    fn maps_rename_to_path_moved() {
        let from = PathBuf::from("/tmp/old.txt");
        let to = PathBuf::from("/tmp/new.txt");
        let events = map_notify_event(
            Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
                .add_path(from.clone())
                .add_path(to.clone()),
        );

        assert_eq!(events, vec![WatcherEvent::PathMoved { from, to }]);
    }

    #[test]
    fn maps_directory_create() {
        let path = PathBuf::from("/tmp/docs");
        let events =
            map_notify_event(Event::new(EventKind::Create(CreateKind::Folder)).add_path(path.clone()));

        assert_eq!(events, vec![WatcherEvent::DirectoryCreated(path)]);
    }

    #[test]
    fn maps_ambiguous_event_to_rescan() {
        let path = PathBuf::from("/tmp/file.txt");
        let events =
            map_notify_event(Event::new(EventKind::Create(CreateKind::Any)).add_path(path));

        assert_eq!(events, vec![WatcherEvent::RescanRequested]);
    }
}
