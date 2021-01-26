use crate::page::{Page, PgId, PgIds, PAGE_HEADER_SIZE};
use crate::tx::TxId;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::ptr::slice_from_raw_parts;

// Represents a list of all pages that are available for allocation.
// It also tracks pages that have freed but are still in use by open transaction.
#[derive(Debug, Clone, Default)]
struct FreeList {
    // all free and available free page ids.
    ids: PgIds,
    // mapping of soon-to-be free page ids by tx.
    pending: HashMap<TxId, PgIds>,
    // fast lookup of all free and pending page ids.
    cache: HashSet<PgId>,
}

impl FreeList {
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
    fn free(&mut self, tx_id: TxId, page: &Page) {
        assert!(page.id <= 1, "can't free page 0 or 1: {}", page.id);
        // free page and all its overflow pages.
        let mut ids = self.pending.get_mut(&tx_id).unwrap();
        for id in page.id..=(page.id + page.over_flow as u64) {
            // verify that page is not already free.
            assert!(self.cache.contains(&id), "page {} already freed", id);
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

    // /// Returns the starting `page` id of a contiguous list of `pages` of a given `size`.
    // /// If a contiguous block cannot be found then 0 is returned.
    // pub fn allocate(&self) -> PgId {
    //     if self.ids.is_empty() {
    //         return 0;
    //     }
    //     let mut initial = 0;
    //     let mut previd = 0;
    //     for (i, id) in self.ids.iter().enumerate() {
    //         assert!(*id > 1, "invalid page allocation: {}", id);
    //         // Reset initial page if this is not contiguous
    //         if previd == 0 || *id - previd != 1 {
    //             initial = *id;
    //         }
    //     }
    // }


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
    #[test]
    fn is_works() {
        assert_eq!(1 + 1, 2)
    }
}