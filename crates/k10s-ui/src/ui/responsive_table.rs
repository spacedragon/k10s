/// Unambiguous result of activating a row in a selectable list.
pub(super) enum RowAction<I> {
    Select(I),
    ClearSelection,
}
