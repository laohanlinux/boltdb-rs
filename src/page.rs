const MIN_KEYS_PER_PAGE: u64 = 2;

// const BRANCH_PAGE_ELEMENT_SIZE =

const BRANCH_PAGE_FLAG: u64 = 0x01;
const LEAF_PAGE_FLAG: u64 = 0x02;
const META_PAGE_FLAG: u64 = 0x04;
const FREE_LIST_PAGE_FLAG: u64 = 0x10;

const BUCKET_LEAF_FLAG: u64 = 0x10;


type PgId = u64;

pub struct Page {
    pub id: PgId,
    pub flags: u16,
    pub count: u16,
    pub over_flow: u16,
}

