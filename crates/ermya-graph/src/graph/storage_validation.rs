use super::{
    DataFile, EDGE_SLOT_SIZE, Error, Graph, HashMap, HashSet, NODE_SLOT_SIZE, PAGE_HEADER_SIZE,
    PageHeader, PageType, Result, SLOT_LIVE, SLOTS_PER_PAGE, edge_codec, node_codec,
};
use crate::storage::backend::ALL_DATA_FILES;
use crate::storage::free_list::collect_free_pages;

impl Graph {
    /// Reject inherited allocator inconsistencies before allowing new writes.
    pub(super) fn validate_free_references(&self) -> Result<()> {
        let mut free = HashMap::new();
        for file in ALL_DATA_FILES {
            free.insert(
                file,
                collect_free_pages(self.storage.meta(), file, |id| {
                    self.storage.read_page(file, id)
                })?,
            );
        }
        for (file, size) in [
            (DataFile::Nodes, NODE_SLOT_SIZE),
            (DataFile::Edges, EDGE_SLOT_SIZE),
        ] {
            for page_id in 0..self.storage.page_count(file) {
                let page = self.storage.read_page(file, page_id)?;
                // Free-directory contents are allocator metadata, not record slots.
                if PageHeader::read_from(&page).page_type == PageType::FreeDirectory as u16 {
                    continue;
                }
                for bytes in page[PAGE_HEADER_SIZE..]
                    .chunks_exact(size)
                    .take(SLOTS_PER_PAGE)
                {
                    if bytes[0] != SLOT_LIVE {
                        continue;
                    }
                    if free[&file].contains(&page_id) {
                        return Err(Error::CorruptPage {
                            file: file.file_name(),
                            page_id,
                            reason: "live record page is listed as free",
                        });
                    }
                    let reference = if file == DataFile::Nodes {
                        let slot = bytes.try_into().expect("node slot size");
                        node_codec::slot_needs_prop_resolve(slot)
                            .then(|| node_codec::slot_prop_overflow_ref(slot))
                    } else {
                        let slot = bytes.try_into().expect("edge slot size");
                        edge_codec::edge_slot_needs_prop_resolve(slot)
                            .then(|| edge_codec::edge_slot_prop_overflow_ref(slot))
                    };
                    if let Some(id) = reference {
                        self.validate_property_chain(id, &free[&DataFile::Overflow])?;
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_property_chain(&self, first: u32, free: &HashSet<u32>) -> Result<()> {
        let mut current = first;
        let mut seen = HashSet::new();
        loop {
            if free.contains(&current) || !seen.insert(current) {
                return Err(Error::CorruptPage {
                    file: "overflow.db",
                    page_id: current,
                    reason: "live property reference is free or cyclic",
                });
            }
            let page = self.storage.read_page(DataFile::Overflow, current)?;
            if PageHeader::read_from(&page).page_type == PageType::PropertySlab as u16 {
                return Ok(());
            }
            let offset = if current == first {
                4092
            } else {
                PAGE_HEADER_SIZE
            };
            let next =
                u32::from_le_bytes(page[offset..offset + 4].try_into().expect("page link size"));
            if next == u32::MAX {
                return Ok(());
            }
            current = next;
        }
    }
}
