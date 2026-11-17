use std::cell::Cell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    NodeWrite,
    PagesFlush,
    MetaFlush,
    CheckpointSync,
    WalTruncate,
}
thread_local! { static ARMED: Cell<Option<Point>> = const { Cell::new(None) }; }
pub fn arm(point: Point) {
    ARMED.set(Some(point));
}
pub fn hit(point: Point) -> crate::Result<()> {
    if ARMED.get() == Some(point) {
        ARMED.set(None);
        return Err(
            std::io::Error::other(format!("injected persistence failure: {point:?}")).into(),
        );
    }
    Ok(())
}
