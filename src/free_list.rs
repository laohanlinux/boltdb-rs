use crate::page::{Page, PgId, PgIds, FREE_LIST_PAGE_FLAG, PAGE_HEADER_SIZE};
use crate::tx::TxId;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;

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
        unsafe { PAGE_HEADER_SIZE + size_of::<PgId>() * n }
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
    fn allocate(&mut self, n: usize) -> Option<PgId> {
        if self.ids.len() == 0 {
            return None;
        }
        let mut initial = 0;
        let mut prev_id = 0;
        let mut drain: Vec<u64> = vec![];

        for (i, id) in self.ids.iter().enumerate() {
            assert!(*id >= 1, "invalid page allocation: {}", id);

            // Reset initial page if this is not contiguous.
            if prev_id == 0 || id - prev_id != 1 {
                initial = *id;
            }

            // If we found a contiguous block then remove it and return it.
            if (*id - initial) + 1 == n as PgId {
                // If we're allocating off the beginning then take the fast path
                // and just adjust the existing slice. This will use extra memory
                // temporarily but the append() in the free() will realloc the slice
                // as is necessary.
                let mut cur = self.clone();
                if (i + 1) == n {
                    drain = self.ids.drain(..=i);
                } else {
                    drain = self.ids.drain((*id - initial + 1) as usize..=i);
                }

                // Remove from the free cache
                for id in drain {
                    self.cache.remove(&id);
                }
                return Some(initial);
            }
            prev_id = *id;
        }
        None
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
        let mut m = self
            .pending
            .drain_filter(|key, _| *key <= tx_id)
            .map(|(_, pg_id)| pg_id.to_vec())
            .flatten()
            .collect::<Vec<_>>();
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
mod tests {
    use crate::free_list::FreeList;
    use crate::page::FREE_LIST_PAGE_FLAG;
    use crate::{Page, PgIds};
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use test::Bencher;

    // Ensure that a page is added to a transaction's freelist.
    #[test]
    fn t_free() {
        let mut free_list = FreeList::new();
        free_list.free(
            100,
            &Page {
                id: 12,
                ..Default::default()
            },
        );
        let got = free_list.pending.get(&100).unwrap();
        assert_eq!(got.as_slice(), &vec![12]);
    }

    // Ensure that a page and its overflow is added to a transaction's freelist.
    #[test]
    fn t_freelist_free_overflow() {
        let mut free_list = FreeList::new();
        free_list.free(
            100,
            &Page {
                id: 12,
                over_flow: 3,
                ..Default::default()
            },
        );
        let got = free_list.pending.get(&100).unwrap();
        assert_eq!(got.as_slice(), &vec![12, 13, 14, 15]);
    }

    // Ensure that a free list can find contiguous blocks of pages.
    #[test]
    fn t_freelist_release() {
        let mut free_list = FreeList::new();
        free_list.free(
            100,
            &Page {
                id: 12,
                over_flow: 1,
                ..Default::default()
            },
        );
        free_list.free(
            100,
            &Page {
                id: 9,
                ..Default::default()
            },
        );
        free_list.free(
            102,
            &Page {
                id: 39,
                ..Default::default()
            },
        );
        free_list.release(100);
        free_list.release(101);
        assert_eq!(PgIds::from(vec![9, 12, 13]), free_list.ids);
        free_list.release(102);
        assert_eq!(PgIds::from(vec![9, 12, 13, 39]), free_list.ids);
    }

    // Ensure that a free list cab find contiguous blocks of pages.
    #[test]
    fn t_freelist_allocate() {
        let mut free_list = FreeList::new();
        free_list.ids = PgIds::from(vec![3, 4, 5, 6, 7, 9, 12, 13, 18]);
        let tests = vec![(3, 3), (1, 6), (3, 0), (2, 12), (1, 7), (0, 0), (0, 0)];
        for (input, got) in tests {
            let exp = free_list.allocate(input);
            assert_eq!(exp.or(Some(0)).unwrap(), got);
        }

        assert_eq!(PgIds::from(vec![9, 18]), free_list.ids);

        let tests = vec![(1, 9), (1, 18), (1, 0)];
        for (input, got) in tests {
            let exp = free_list.allocate(input);
            assert_eq!(exp.or(Some(0)).unwrap(), got);
        }
        assert_eq!(PgIds::new(), free_list.ids);
    }

    // Ensure that a free list can deserialize from a free_list page.
    #[test]
    fn t_freelist_read() {
        // Crate a page
        let buf = &mut [0; 4096];
        let mut page = Page::from_slice_mut(buf);
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

    // Ensure that a free_list can serialize into a free_list page.
    #[test]
    fn t_freelist_write() {
        // Create a free_list and write it to a page.
        let buffer = &mut [0; 4096];
        let mut free_list = FreeList::new();
        free_list.ids = PgIds::from(vec![12, 39]);
        free_list.pending.insert(100, PgIds::from(vec![28, 11]));
        free_list.pending.insert(101, PgIds::from(vec![3]));
        let page = Page::from_slice_mut(buffer);
        free_list.write(page);

        // Read the page back out.
        let mut free_list2 = FreeList::new();
        free_list2.read(&page);

        // Ensure that the free_list is correct.
        // All pages should be present and in reverse order.
        let exp = PgIds::from(vec![3, 11, 12, 28, 39]);
        assert_eq!(exp, free_list2.ids);
    }

    #[bench]
    fn b_free_list_release10k(b: &mut Bencher) {
        benchmark_free_list_release(b, 10_000);
    }

    #[bench]
    fn b_free_list_release100k(b: &mut Bencher) {
        benchmark_free_list_release(b, 100_000);
    }

    #[bench]
    fn benchmark_free_list_release1000k(b: &mut Bencher) {
        benchmark_free_list_release(b, 1000_000);
    }

    #[bench]
    fn benchmark_free_list_release10000k(b: &mut Bencher) {
        benchmark_free_list_release(b, 10000_000);
    }

    fn benchmark_free_list_release(b: &mut Bencher, n: usize) {
        let mut rng = rand::thread_rng();
        let ids = random_pgids(n, &mut rng);
        b.iter(move || {
            let mut rng = rng.clone();
            let ids = ids.clone();
            let mut free_list = FreeList::new();
            free_list.ids = ids;
            free_list
                .pending
                .insert(1, random_pgids((free_list.ids.len() / 400) + 1, &mut rng));
            free_list.release(1);
        });
    }

    fn random_pgids(n: usize, rng: &mut ThreadRng) -> PgIds {
        let num = rng.gen_range(0..n);
        let mut pids = Vec::with_capacity(num);
        pids.iter_mut().for_each(|pgid| *pgid = rng.gen());
        PgIds::from(pids)
    }
}
