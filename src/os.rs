#[cfg(target_os = "linux")]
const MAX_MAP_SIZE: usize = 0xFFFFFFFFFFFF;

#[cfg(target_os = "mac")]
const MAX_ALLOC_SIZE: usize = 0xFFFFFFFFFFFF;