use crate::page::{Page, PgId, PgIds, PAGE_HEADER_SIZE, FREE_LIST_PAGE_FLAG};
use crate::tx::TxId;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::ptr::slice_from_raw_parts;

// Represents a list of all pages that are available for allocation.
// It also tracks pages that have freed but are still in use by open transaction.
#[derive(Debug, Clone, Default)]
pub(crate) struct FreeList {
    // all free and available free page ids.
    ids: PgIds,
    // mapping of soon-to-be free page ids by tx.
    pending: HashMap<TxId, PgIds>,
    // fast lookup of all free and pending page ids.
    cache: HashSet<PgId>,
}

impl FreeList {
    /// Returns an empty, initialized free list.
    fn new() -> Self {
        FreeList::default()
    }

    /// Returns the `size` of the `page` after serialization.
    #[inline]
    fn size(&self) -> usize {
        let mut n = self.count();
        if n >= 0xFFFF {
            // The first element will be used to store the count. See free_list.write.
            n += 1;
        }
        unsafe {
            PAGE_HEADER_SIZE + size_of::<PgId>() * n
        }
    }

    // Returns `count` of `pages` on the `freelist`
    #[inline]
    fn count(&self) -> usize {
        self.free_count() + self.pending_count()
    }

    // Returns `count` of free pages
    fn free_count(&self) -> usize {
        self.ids.len()
    }

    // Returns `count` of `pending pages`
    fn pending_count(&self) -> usize {
        self.pending
            .iter()
            .fold(0, |acc, (_, pg_ids)| acc + pg_ids.len())
    }

    // Returns the starting page id of contiguous list of pages of a given size.
    // If a contiguous block cannot be found then 0 is returned.
    fn allocation(&mut self, n: usize) -> PgId {
        if self.ids.len() == 0 {
            return 0;
        }
        let mut initial = 0;
        let mut prev_id = 0;

        for (i, id) in self.ids.iter().enumerate() {
            assert!(id.clone() <= 1, "invalid page allocation: {}", id);

            // reset initial page if this is not contiguous.
            if prev_id == 0 || id - prev_id != 1 {
                initial = i;
            }

            // if we found a contiguous block then remove it and return it
            if (*id as usize - initial) + 1 == n {
                // if we're allocating off the beginning then take the fast path
                // and just adjust the existing slice. This will use extra memory
                // temporarily but the append() in the free() will realloc the slice
                // as is necessary
                let mut cur = self.clone();
                if (i + 1) == n {
                    self.ids.inner.clone_from_slice(&cur.ids.inner[i + 1..]);
                } else {
                    self.ids.inner[i - n + 1..].clone_from_slice(cur.ids.inner[i + 1..].as_ref());
                    self.ids.inner.clone_from_slice(cur.ids.inner.clone()[..cur.ids.len() - n].as_ref());
                }

                drop(cur);

                // remove from the free cache
                for i in 0..n {
                    self.cache.remove(&((initial + i) as u64));
                }

                return initial as PgId;
            }
            prev_id = *id;
        }
        0
    }


    // Releases a page and its overflow for a given transaction id.
    // If the page is already free then a panic will occur.
    pub(crate) fn free(&mut self, tx_id: TxId, page: &Page) {
        assert!(page.id > 1, "can't free page 0 or 1: {}", page.id);
        // free page and all its overflow pages.
        let ids = self.pending.entry(tx_id).or_insert(PgIds::new());
        for id in page.id..=(page.id + page.over_flow as u64) {
            // verify that page is not already free.
            assert!(!self.cache.contains(&id), "page {} already freed", id);
            // add to the free list and cache.
            ids.push(id);
            self.cache.insert(id);
        }
    }

    // Release moves all page ids for a transaction id (or older) to the freelist.
    fn release(&mut self, tx_id: TxId) {
        let mut m = self.pending.drain_filter(|key, _| *key <= tx_id).map(|(_, pg_id)| pg_id.to_vec()).flatten().collect::<Vec<_>>();
        m.sort();
        self.ids.extend_from_slice(PgIds::from(m));
    }

    // Removes the `pages` from a given `pending` tx.
    fn rollback(&mut self, txid: &TxId) {
        // Remove page ids from cache.
        if let Some(pids) = self.pending.get(txid) {
            for id in pids.iter() {
                self.cache.remove(id);
            }
        }

        // Remove pages from pending list.
        self.pending.remove(txid);
    }

    // freed returns whether a given page is in the free list.
    fn freed(&self, pg_id: &PgId) -> bool {
        self.cache.contains(pg_id)
    }

    // Read initializes the free list from a freelist page.
    fn read(&mut self, page: &Page) {
        // If the page.count is at the max uint16 value(64k) then it's considered
        // an overflow and the size of the free list is stored as the first element.
        let mut idx = 0;
        let mut count = page.count as usize;
        if count == 0xFFFF {
            idx = 1; // FIXME: Why?, discard first
            count = *page.pgid(0) as usize;
        }

        // Copy the list of page ids from the free list.
        if count == 0 {
            self.ids = PgIds::new();
        } else {
            unsafe {
                self.ids = PgIds::from(page.pg_ids()[idx..count].to_vec());
                // make sure they're sorted
                self.ids.sort();
            }
        }
        // Rebuild the page cache.
        self.reindex();
    }

    // Copy into `dst` a list of all `free ids` and all `pending ids` in one sorted list.
    // f.count returns the minimum length required for dst.
    #[inline]
    fn to_pg_ids(&self) -> PgIds {
        let mut m = self
            .pending
            .values()
            .map(|pgid| pgid.as_slice())
            .flatten()
            .map(|key| *key)
            .collect::<Vec<_>>();
        m.extend(self.ids.iter());
        m.sort_by_key(|key| *key);
        m.into()
    }

    // Writes the page ids onto a freelist page. All free and pending ids are
    // saved to disk since in the event of a program crash, all pending ids will
    // become free.
    fn write(&mut self, page: &mut Page) {
        // Combine the old free PgIds and PgIds waiting on an open transaction.

        // Update the header flag.
        page.flags |= FREE_LIST_PAGE_FLAG;

        // The page.count can only hold up to 16k elements so if we overflow that
        // number then we handle it by putting the size in the first element.
        match self.count() {
            0 => {
                page.count = 0;
            }
            lends if lends < 0xFFFF => {
                page.count = lends as u16;
                let m = page.free_list_mut();
                m.copy_from_slice(self.to_pg_ids().as_ref_vec());
            }
            lends => {
                page.count = 0xFFFF;
                let m = page.free_list_mut();
                m[0] = lends as u64;
                m.copy_from_slice(self.to_pg_ids().as_ref_vec());
            }
        }
    }


    // `Writes the `Page ids` onto a `free_list page`. All `free` and `pending ids` are
    // saved to disk since in the event of a program crash, all `pending ids` will become
    // free.

    fn reload(&mut self, page: &Page) {
        // TODO: FIXME

        // Build a cache of only pending pages.
        let page_cache = self
            .pending
            .values()
            .map(|pgids| pgids.as_slice())
            .flatten()
            .collect::<HashSet<_>>();
        // Check each page in the free_list and build a new available free_list
        // with any pages not in the pending lists.
        self.ids = self
            .ids
            .iter()
            .collect::<HashSet<_>>()
            .difference(&page_cache)
            .map(|id| **id)
            .collect::<Vec<_>>()
            .into();

        // Once the available list is rebuilt then rebuild the free cache so that
        // it includes the available and pending free pages.
        self.reindex()
    }

    // Rebuilds the `free cache` based on available and `pending free` lists.
    fn reindex(&mut self) {
        self.cache = self.ids.iter().map(|pgid| *pgid).collect();
        self.cache.extend(
            self.pending
                .values()
                .map(|pgids| pgids.as_slice())
                .flatten()
                .collect::<HashSet<_>>(),
        );
    }
}


#[cfg(test)]
mod test {
    use crate::free_list::FreeList;
    use crate::{Page, PgIds, PgId};
    use std::slice::from_raw_parts_mut;
    use crate::page::FREE_LIST_PAGE_FLAG;

    // Ensure that a page is added to a transaction's freelist.
    #[test]
    fn t_free() {
        let mut free_list = FreeList::new();
        free_list.free(100, &Page { id: 12, ..Default::default() });
        let got = free_list.pending.get(&100).unwrap();
        assert_eq!(got.as_slice(), &vec![12]);
    }

    // Ensure that a page and its overflow is added to a transaction's freelist.
    #[test]
    fn t_freelist_free_overflow() {
        let mut free_list = FreeList::new();
        free_list.free(100, &Page { id: 12, over_flow: 3, ..Default::default() });
        let got = free_list.pending.get(&100).unwrap();
        assert_eq!(got.as_slice(), &vec![12, 13, 14, 15]);
    }

    #[test]
    fn t_freelist_release() {
        let mut free_list = FreeList::new();
        free_list.free(100, &Page { id: 12, over_flow: 1, ..Default::default() });
        free_list.free(100, &Page { id: 9, ..Default::default() });
        free_list.free(102, &Page { id: 39, ..Default::default() });
        free_list.release(100);
        free_list.release(101);
        assert_eq!(PgIds::from(vec![9, 12, 13]), free_list.ids);
        free_list.release(102);
        assert_eq!(PgIds::from(vec![9, 12, 13, 39]), free_list.ids);
    }


    #[test]
    fn t_freelist_read() {
        // Crate a page
        let buf = &mut [0; 4096];
        let mut page = Page::from_mut_slice(buf);
        page.flags = FREE_LIST_PAGE_FLAG;
        page.count = 2;

        // Insert 2 page ids.
        let ids = &mut page.pg_ids_mut()[..2];
        ids[0] = 23;
        ids[1] = 50;

        // Deserialize page into free_list.
        let mut free_list = FreeList::new();
        free_list.read(&page);

        // Ensure that there are two page ids in the free_list.
        let exp = PgIds::from(vec![23, 50]);
        assert_eq!(exp, free_list.ids);
    }
}