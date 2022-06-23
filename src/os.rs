#[cfg(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
    target_arch = "mips64"
))]
pub const MAX_MMAP_SIZE: u64 = 0xFFFFFFFFFFFF; // 256T

#[cfg(any(
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "powerpc"
))]
pub const MAX_MMAP_SIZE: u64 = 0x7FFFFFFF; // 2GB
