//! Read-only B+tree traversal.

use anyhow::{Result, bail, ensure};

use super::page::{Page, PageType};

/// The maximum number of branch levels followed by a lookup.
///
/// This is a safety bound against corrupt or cyclic pages, not a format limit.
const MAX_DEPTH: usize = 64;

/// A source of validated pages addressed by page ID.
pub(crate) trait PageSource {
    /// Returns the page with the given ID.
    fn page(&self, page_id: u64) -> Result<Page<'_>>;
}

/// Looks up `key` in the tree rooted at `root`.
///
/// A root of 0 represents an empty tree. Values are returned as a slice into
/// the page source, so they keep the source's lifetime.
pub(crate) fn get<'a, S: PageSource>(
    source: &'a S,
    root: u64,
    key: &[u8],
) -> Result<Option<&'a [u8]>> {
    if root == 0 {
        return Ok(None);
    }

    let mut page_id = root;
    for _ in 0..MAX_DEPTH {
        let page = source.page(page_id)?;
        match page.page_type() {
            PageType::Branch => {
                page_id = page.branch_child(key)?;
                ensure!(page_id >= 2, "branch points at reserved page ID {page_id}");
            }
            PageType::Leaf => return page.leaf_get(key),
        }
    }

    bail!("B+tree depth exceeded {MAX_DEPTH}; the page chain may contain a cycle")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Context;

    use super::*;
    use crate::storage::PAGE_SIZE;
    use crate::storage::page::{build_branch, build_leaf};

    struct MemorySource {
        pages: BTreeMap<u64, [u8; PAGE_SIZE]>,
    }

    impl MemorySource {
        fn new() -> Self {
            Self {
                pages: BTreeMap::new(),
            }
        }

        fn insert(&mut self, page_id: u64, page: [u8; PAGE_SIZE]) {
            self.pages.insert(page_id, page);
        }
    }

    impl PageSource for MemorySource {
        fn page(&self, page_id: u64) -> Result<Page<'_>> {
            let page = self
                .pages
                .get(&page_id)
                .with_context(|| format!("page {page_id} not found"))?;
            Page::parse(page, Some(page_id))
        }
    }

    #[test]
    fn empty_tree_has_no_values() {
        let source = MemorySource::new();

        assert_eq!(get(&source, 0, b"key").unwrap(), None);
    }

    #[test]
    fn finds_values_in_a_single_leaf() {
        let mut source = MemorySource::new();
        source.insert(
            2,
            build_leaf(2, 1, &[(b"a", b"1"), (b"c", b"3"), (b"e", b"5")]).unwrap(),
        );

        assert_eq!(get(&source, 2, b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(get(&source, 2, b"c").unwrap(), Some(&b"3"[..]));
        assert_eq!(get(&source, 2, b"e").unwrap(), Some(&b"5"[..]));
        assert_eq!(get(&source, 2, b"b").unwrap(), None);
        assert_eq!(get(&source, 2, b"z").unwrap(), None);
    }

    #[test]
    fn traverses_branch_pages() {
        let mut source = MemorySource::new();
        source.insert(
            2,
            build_leaf(2, 1, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]).unwrap(),
        );
        source.insert(
            3,
            build_leaf(3, 1, &[(b"d", b"4"), (b"e", b"5"), (b"f", b"6")]).unwrap(),
        );
        source.insert(
            4,
            build_leaf(4, 1, &[(b"g", b"7"), (b"h", b"8"), (b"i", b"9")]).unwrap(),
        );
        source.insert(
            5,
            build_branch(5, 1, &[(b"", 2), (b"d", 3), (b"g", 4)]).unwrap(),
        );

        assert_eq!(get(&source, 5, b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(get(&source, 5, b"e").unwrap(), Some(&b"5"[..]));
        assert_eq!(get(&source, 5, b"i").unwrap(), Some(&b"9"[..]));
        assert_eq!(get(&source, 5, b"c1").unwrap(), None);
        assert_eq!(get(&source, 5, b"z").unwrap(), None);
    }

    #[test]
    fn rejects_cycles() {
        let mut source = MemorySource::new();
        source.insert(2, build_branch(2, 1, &[(b"", 2)]).unwrap());

        let error = get(&source, 2, b"key").unwrap_err();
        assert!(error.to_string().contains("depth exceeded"));
    }

    #[test]
    fn matches_btreemap_in_a_single_leaf() {
        let mut map = BTreeMap::new();
        for i in 0..200u32 {
            map.insert(i.to_be_bytes().to_vec(), vec![i as u8]);
        }

        let entries: Vec<(&[u8], &[u8])> = map
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect();
        let mut source = MemorySource::new();
        source.insert(2, build_leaf(2, 1, &entries).unwrap());

        for (key, value) in &map {
            assert_eq!(get(&source, 2, key).unwrap(), Some(value.as_slice()));
        }
        for i in 200..300u32 {
            assert_eq!(get(&source, 2, &i.to_be_bytes()).unwrap(), None);
        }
    }
}
