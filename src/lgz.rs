use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt, LE};
use hashbrown::HashMap;
use std::io::Read;
use std::io::Write;
use std::mem::size_of;
#[cfg(feature = "debug")]
use std::sync::atomic::{AtomicUsize, Ordering};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, LE as ZLE, U16, U32, U64};

const ENABLE_EXPRISH: bool = true;

#[cfg(feature = "debug")]
pub struct Stats {
  pub normal: [AtomicUsize; 256],
  pub exprish: [AtomicUsize; 256],
}

#[cfg(feature = "debug")]
pub static STATS: Stats = {
  #[allow(clippy::declare_interior_mutable_const)]
  const NEW: AtomicUsize = AtomicUsize::new(0);
  Stats { normal: [NEW; 256], exprish: [NEW; 256] }
};

fn use_gmp(mac_v2: bool) -> bool {
  if cfg!(feature = "gmp") {
    true
  } else if cfg!(feature = "no-gmp") {
    false
  } else if cfg!(target_family = "unix") && cfg!(target_arch = "x86_64") {
    true
  } else if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
    !mac_v2
  } else {
    false
  }
}

enum OLeanVersion {
  V0,
  V1,
  V2,
}

#[repr(C, align(8))]
#[derive(KnownLayout, Immutable, FromBytes, IntoBytes)]
struct HeaderV0 {
  magic: [u8; 16],
  base: U64<ZLE>,
  root: U64<ZLE>,
}

#[derive(PartialOrd, Ord, PartialEq, Eq)]
struct RegularLeanVersion {
  major: u8,
  minor: u8,
  patch: u8,
  /// Numbers are shifted so that lex order works:
  /// * release candidate N: `rc_m1 = N-1`
  /// * stable: `rc_m1 = 0xFF`
  rc_m1: u8,
}

enum LeanVersion {
  Regular(RegularLeanVersion),
  Unknown([u8; 33]),
}

impl LeanVersion {
  const UNKNOWN: Self = Self::Unknown([0; 33]);

  fn parse(lean_version: &[u8; 33]) -> LeanVersion {
    (|| -> Option<RegularLeanVersion> {
      let s = lean_version.strip_prefix(b"4.")?;
      let end = s.iter().position(|&c| c == b'.')?;
      let minor: u8 = std::str::from_utf8(&s[..end]).ok()?.parse().ok()?;
      let s = &s[end + 1..];
      let end = s.iter().position(|&c| c == 0).unwrap_or(s.len());
      let s = &s[..end];
      let end = s.iter().position(|&c| c == b'-').unwrap_or(s.len());
      let patch: u8 = std::str::from_utf8(&s[..end]).ok()?.parse().ok()?;
      let rc: u8 = if let Some(rest) = s[end..].strip_prefix(b"-rc") {
        std::str::from_utf8(rest).ok()?.parse().ok()?
      } else if s[end..].is_empty() {
        0
      } else {
        return None
      };
      Some(RegularLeanVersion { major: 4, minor, patch, rc_m1: rc.wrapping_sub(1) })
    })()
    .map_or_else(|| LeanVersion::Unknown(*lean_version), LeanVersion::Regular)
  }

  fn encode(major: u8, minor: u8, patch: u8, rc_m1: u8) -> String {
    if rc_m1 == u8::MAX {
      format!("{major}.{minor}.{patch}")
    } else {
      format!("{major}.{minor}.{patch}-rc{}", rc_m1 + 1)
    }
  }
}
struct Header {
  lean_version: LeanVersion,
  githash: [u8; 40],
  base: u64,
  root: u64,
}

fn parse_as_v0(olean: &[u8]) -> (Config, Header, u64, &[u8]) {
  let (header, rest) = Ref::<_, HeaderV0>::from_prefix(olean).expect("bad header");
  assert_eq!(&header.magic, b"oleanfile!!!!!!!");
  let base = header.base.get();
  let offset = base + size_of::<HeaderV0>() as u64;
  let cfg = Config { use_gmp: use_gmp(false) };
  let lean_version = LeanVersion::UNKNOWN;
  (cfg, Header { base, lean_version, githash: [0; 40], root: header.root.get() }, offset, rest)
}

#[repr(C, align(8))]
#[derive(KnownLayout, Immutable, FromBytes)]
struct HeaderV1 {
  magic: [u8; 5],
  version: u8,
  githash: [u8; 40],
  reserved: [u8; 2],
  base: U64<ZLE>,
  root: U64<ZLE>,
}

fn mac_v1_use_gmp(githash: &[u8; 40]) -> bool {
  if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
    matches!(
      githash,
      b"be6c4894e0a6c542d56a6f4bb1238087267d21a0" // v4.9.0-rc1
      | b"7ed9b73f4d4c994b603cd369758c79eacdafc62f" // v4.9.0-rc2
      | b"141856d6e6d808a85b9147a530294fee8e48e15f" // v4.9.0-rc3
      | b"8f9843a4a5fe1b0c2f24c74097f296e2818771ee" // v4.9.0
      | b"1b78cb4836cf626007bd38872956a6fab8910993" // v4.9.1
      | b"3b58e0649156610ce3aeed4f7b5c652340c668d4" // v4.10.0-rc1
      | b"702c31b8071269f0052fd1e0fb3891a079a655bd" // v4.10.0-rc2
      | b"c375e19f6b656fcd594cdca3a38b8578634df8cd" // v4.10.0
      | b"daa22187642d4cf6954c39a23eab20d8a8675416" // v4.11.0-rc1
      | b"0edf1bac392f7e2fe0266b28b51c498306363a84" // v4.11.0-rc2
      | b"c122849f0759797734971b6ff4dfa82f01c653c6" // v4.11.0-rc3
      | b"ec3042d94bd11a42430f9e14d39e26b1f880f99b" // v4.11.0
      | b"e9e858a4484905a0bfe97c4f05c3924ead02eed8" // v4.12.0-rc1
      | b"dc2533473114eb8656439ff2b9335209784aa640" // v4.12.0
      | b"4cb90dddfcb8ceda2b89711d567d593e1fd07090" // v4.13.0-rc1
      | b"b1b73a444f9b13c003ad9dd05881c44e9861a827" // v4.13.0-rc2
      | b"01d414ac36dc28f3e424dabd36d818873fea655c" // v4.13.0-rc3
      | b"480d7314a2c499f670609b2c2623a79d36cea760" // v4.13.0-rc4
      | b"6d22e0e5cc5a4392466e3d6dd8522486d1fd038b" // v4.13.0
    )
  } else {
    false
  }
}

fn parse_as_v1(olean: &[u8]) -> (Config, Header, u64, &[u8]) {
  let (header, rest) = Ref::<_, HeaderV1>::from_prefix(olean).expect("bad header");
  assert_eq!(&header.magic, b"olean");
  assert_eq!(header.version, 1);
  assert_eq!(header.reserved, [0; 2]);
  let base = header.base.get();
  let offset = base + size_of::<HeaderV1>() as u64;
  let mut githash = [0; 40];
  githash.copy_from_slice(&header.githash);
  let cfg = Config { use_gmp: use_gmp(mac_v1_use_gmp(&header.githash)) };
  let lean_version = LeanVersion::UNKNOWN;
  (cfg, Header { base, lean_version, githash, root: header.root.get() }, offset, rest)
}

#[repr(C, align(8))]
#[derive(KnownLayout, Immutable, FromBytes)]
struct HeaderV2 {
  magic: [u8; 5],
  version: u8,
  flags: u8,
  lean_version: [u8; 33],
  githash: [u8; 40],
  base: U64<ZLE>,
  root: U64<ZLE>,
}

fn parse_as_v2(olean: &[u8]) -> (Config, Header, u64, &[u8]) {
  let (header, rest) = Ref::<_, HeaderV2>::from_prefix(olean).expect("bad header");
  assert_eq!(&header.magic, b"olean");
  assert_eq!(header.version, 2);
  assert_eq!(header.flags & !1, 0);
  let base = header.base.get();
  let offset = base + size_of::<HeaderV2>() as u64;
  let mut githash = [0; 40];
  githash.copy_from_slice(&header.githash);
  let cfg = Config { use_gmp: header.flags != 0 };
  let lean_version = LeanVersion::parse(&header.lean_version);
  (cfg, Header { base, lean_version, githash, root: header.root.get() }, offset, rest)
}

fn sniff_olean_version(olean: &[u8]) -> OLeanVersion {
  if Ref::<_, HeaderV0>::from_prefix(olean)
    .is_ok_and(|(header, _)| header.magic == *b"oleanfile!!!!!!!")
  {
    return OLeanVersion::V0
  }
  match Ref::<_, HeaderV1>::from_prefix(olean).expect("bad header").0.version {
    1 => OLeanVersion::V1,
    2 => OLeanVersion::V2,
    v => panic!("unexpected olean version: {v}"),
  }
}

#[derive(Clone, Copy, Default)]
struct Config {
  use_gmp: bool,
}

pub fn compress(olean: &[u8], outfile: impl Write) {
  let version = sniff_olean_version(olean);

  let (cfg, Header { githash, lean_version, base, root }, offset, rest) = match version {
    OLeanVersion::V0 => parse_as_v0(olean),
    OLeanVersion::V1 => parse_as_v1(olean),
    OLeanVersion::V2 => parse_as_v2(olean),
  };
  assert!(base & !((u32::MAX as u64) << 16) == 0);
  let mut refs = vec![0u8; rest.len() >> 3];
  let mut pos = 0;
  while pos < rest.len() {
    pos = on_subobjs(cfg, rest, pos, |ptr| {
      let b = &mut refs[(ptr - offset) as usize >> 3];
      *b = b.saturating_add(1);
    });
    pos = pad_to(pos, 8).1;
  }
  let mut w = LgzWriter {
    cfg,
    file: WithPosition { r: outfile, pos: 0 },
    refs: &refs,
    offset,
    backrefs: Default::default(),
    buf: rest,
    depth: 0,
  };
  match version {
    OLeanVersion::V0 => {
      w.file.write_all(&MAGIC0).unwrap();
      w.file.write_u32::<LE>((base >> 16) as u32).unwrap();
      w.file.write_u64::<LE>(olean.len() as u64).unwrap();
    }
    OLeanVersion::V1 => {
      w.file.write_all(&MAGIC1).unwrap();
      w.file.write_u32::<LE>((base >> 16) as u32).unwrap();
      w.file.write_u64::<LE>(olean.len() as u64).unwrap();
      w.file.write_all(&githash).unwrap();
    }
    OLeanVersion::V2 => {
      w.file.write_all(&MAGIC2).unwrap();
      w.file.write_u32::<LE>((base >> 16) as u32).unwrap();
      w.file.write_u64::<LE>(olean.len() as u64).unwrap();
      w.file.write_all(&githash).unwrap();
      w.file.write_u8(cfg.use_gmp as u8).unwrap();
      match lean_version {
        LeanVersion::Regular(v) => {
          w.file.write_all(&[v.major, v.minor, v.patch, v.rc_m1]).unwrap();
        }
        LeanVersion::Unknown(v) => {
          w.file.write_u8(0).unwrap();
          w.file.write_all(&v).unwrap();
        }
      }
    }
  }
  w.write_obj(root, LgzMode::Normal);
}

#[repr(C, align(8))]
#[derive(KnownLayout, Immutable, FromBytes, IntoBytes)]
struct ObjHeader {
  rc: U32<ZLE>,
  cs_sz: U16<ZLE>,
  num_fields: u8,
  tag: u8,
}
impl ObjHeader {
  fn sfields(&self) -> u16 { (self.cs_sz.get() >> 3).saturating_sub(self.num_fields as u16 + 1) }
}

macro_rules! bin_parser {
  ($(($name: ident, $i_name: ident, $t:ident, $i_t:ident))*) => {
    $(
      fn $name(buf: &[u8], pos: usize) -> ($t, usize) {
        let int_bytes = buf
          .get(pos..(pos + size_of::<$t>()))
          .unwrap_or_else(|| panic!("out of range"));
        assert_eq!(int_bytes.len(), size_of::<$t>());
        ($t::from_le_bytes(int_bytes.try_into().unwrap()), pos + size_of::<$t>())
      }
      #[allow(unused)]
      fn $i_name(buf: &[u8], pos: usize) -> ($i_t, usize) {
        let (val, pos) = $name(buf, pos);
        (val as $i_t, pos)
      }
    )*
  };
}

bin_parser! {
  (parse_u8,  parse_i8,  u8,  i8)
  (parse_u16, parse_i16, u16, i16)
  (parse_u32, parse_i32, u32, i32)
  (parse_u64, parse_i64, u64, i64)
}

fn pad_to(pos: usize, n: u8) -> (usize, usize) {
  let i = (n.wrapping_sub(pos as u8) & (n - 1)).into();
  (i, pos + i)
}

mod tag {
  pub(crate) const PROMISE: u8 = 244;
  pub(crate) const CLOSURE: u8 = 245;
  pub(crate) const ARRAY: u8 = 246;
  pub(crate) const STRUCT_ARRAY: u8 = 247;
  pub(crate) const SCALAR_ARRAY: u8 = 248;
  pub(crate) const STRING: u8 = 249;
  pub(crate) const MPZ: u8 = 250;
  pub(crate) const THUNK: u8 = 251;
  pub(crate) const TASK: u8 = 252;
  pub(crate) const REF: u8 = 253;
  pub(crate) const EXTERNAL: u8 = 254;
  pub(crate) const RESERVED: u8 = 255;
}

macro_rules! on_leanobj {
  ($f:expr, $ptr:ident) => {
    if $ptr & 1 == 0 {
      $f($ptr);
    }
  };
}

fn parse<T: KnownLayout + Immutable>(buf: &[u8], pos: usize) -> (Ref<&[u8], T>, usize) {
  let t = Ref::<_, T>::from_prefix(&buf[pos..]).expect("bad header").0;
  (t, pos + size_of::<T>())
}

fn on_subobjs(cfg: Config, buf: &[u8], pos0: usize, mut f: impl FnMut(u64)) -> usize {
  let (header, pos) = parse::<ObjHeader>(buf, pos0);
  assert!(header.rc.get() == 0);
  match header.tag {
    tag::ARRAY => {
      let (size, pos) = parse_u64(buf, pos);
      let (capacity, pos) = parse_u64(buf, pos);
      assert!(size == capacity && header.cs_sz.get() == 1 && header.num_fields == 0);
      on_array_subobjs(buf, size, pos, f)
    }
    tag::SCALAR_ARRAY => {
      let (size, pos) = parse_u64(buf, pos);
      let (capacity, pos) = parse_u64(buf, pos);
      assert!(header.cs_sz.get() == 1 && size == capacity);
      pos + capacity as usize
    }
    tag::STRING => {
      let (size, pos) = parse_u64(buf, pos);
      let (capacity, pos) = parse_u64(buf, pos);
      assert!(header.cs_sz.get() == 1 && size == capacity);
      let (_length, pos) = parse_u64(buf, pos);
      pos + capacity as usize
    }
    tag::MPZ =>
      if cfg.use_gmp {
        let (capacity, pos) = parse_u32(buf, pos);
        let (size, pos) = parse_i32(buf, pos);
        assert!(
          header.cs_sz.get() == ((capacity + 3) as u16) << 3
            && size.unsigned_abs() == capacity
            && capacity != 0
        );
        let (_limbs_ptr, pos) = parse_u64(buf, pos);
        pos + (8 * capacity) as usize
      } else {
        let (sign, pos) = parse_u64(buf, pos);
        let (size, pos) = parse_u64(buf, pos);
        assert!(header.cs_sz.get() == ((size + 8) as u16) << 2 && sign < 2 && size != 0);
        let (_limbs_ptr, pos) = parse_u64(buf, pos);
        pos + (4 * size) as usize
      },
    tag::THUNK | tag::TASK => {
      let (value, pos) = parse_u64(buf, pos);
      let (imp, pos) = parse_u64(buf, pos);
      assert!(header.cs_sz.get() == 3 << 3 && imp == 0);
      on_leanobj!(f, value);
      pos
    }
    tag::REF | tag::PROMISE => {
      let (value, pos) = parse_u64(buf, pos);
      assert!(header.cs_sz.get() == 2 << 3);
      on_leanobj!(f, value);
      pos
    }
    tag::CLOSURE => panic!("closure"),
    tag::STRUCT_ARRAY => panic!("struct array"),
    tag::EXTERNAL => panic!("external"),
    tag::RESERVED => panic!("reserved"),
    _ctor => {
      let len_except_sfields = 8 + 8 * header.num_fields as usize;
      assert!(len_except_sfields <= header.cs_sz.get() as usize && header.cs_sz.get() & 7 == 0);
      on_array_subobjs(buf, header.num_fields.into(), pos, f);
      pos0 + header.cs_sz.get() as usize
    }
  }
}

fn on_array_subobjs(buf: &[u8], size: u64, mut pos: usize, mut f: impl FnMut(u64)) -> usize {
  for _ in 0..size {
    let (ptr, pos2) = parse_u64(buf, pos);
    on_leanobj!(f, ptr);
    pos = pos2
  }
  pos
}

struct LgzWriter<'a, W> {
  cfg: Config,
  buf: &'a [u8],
  file: WithPosition<W>,
  depth: usize,
  offset: u64,
  refs: &'a [u8],
  backrefs: HashMap<u64, u32>,
}

pub const NAME_ANON_HASH: u64 = 1723;

pub(crate) const MAGIC0: [u8; 4] = *b"LGZ!";
pub(crate) const MAGIC1: [u8; 4] = *b"LGZ1";
pub(crate) const MAGIC2: [u8; 4] = *b"LGZ2";

// These are valid in normal and exprish mode
pub(crate) const UINT0: u8 = 0xd0;
pub(crate) const UINT0_END: u8 = 0xef;
pub(crate) const BACKREF0: u8 = 0xf0;
pub(crate) const BACKREF0_END: u8 = 0xf7;
pub(crate) const INT1: u8 = 0xf8;
pub(crate) const INT2: u8 = 0xf9;
pub(crate) const INT4: u8 = 0xfa;
pub(crate) const INT8: u8 = 0xfb;
pub(crate) const BACKREF1: u8 = 0xfc;
pub(crate) const BACKREF2: u8 = 0xfd;
pub(crate) const BACKREF4: u8 = 0xfe;
pub(crate) const SAVE: u8 = 0xff;

// only valid in normal mode
pub(crate) const ARRAY: u8 = 0x00;
pub(crate) const BIG_CTOR: u8 = 0x10;
pub(crate) const SCALAR_ARRAY: u8 = 0x20;
pub(crate) const STRING: u8 = 0x30;
pub(crate) const MPZ: u8 = 0x40;
pub(crate) const THUNK: u8 = 0x50;
pub(crate) const TASK: u8 = 0x60;
pub(crate) const REF: u8 = 0x70;
pub(crate) const EXPRISH: u8 = 0x80;
pub(crate) const PROMISE: u8 = 0x90;

// only valid in exprish mode
pub(crate) mod exprish {
  use super::mix_hash;

  pub(crate) const NORMAL: u8 = 0x20;

  pub(crate) const NAME_STR: u8 = 0x00;
  pub(crate) const NAME_NUM: u8 = 0x01;

  pub(crate) const LEVEL_SUCC: u8 = 0x02;
  pub(crate) const LEVEL_MAX: u8 = 0x03;
  pub(crate) const LEVEL_IMAX: u8 = 0x04;
  pub(crate) const LEVEL_PARAM: u8 = 0x05;
  pub(crate) const LEVEL_MVAR: u8 = 0x06;
  pub(crate) const fn pack_level_data(h: u32, depth: u32, bits: u8) -> u64 {
    (h as u64) | ((bits as u64) << 32) | ((depth as u64) << 40)
  }
  pub(crate) const fn unpack_level_data(data: u64) -> (u32, u32, u8) {
    (data as u32, (data >> 40) as u32, (data >> 32) as u8)
  }

  pub(crate) const LEVEL_ZERO_DATA: u64 = pack_level_data(2221, 0, 0);
  pub(crate) const fn level_succ_data(data: u64) -> u64 {
    let (h, depth, bits) = unpack_level_data(data);
    pack_level_data(mix_hash(2243, h as u64) as u32, depth + 1, bits)
  }
  pub(crate) fn level_data_binary(salt: u64, data1: u64, data2: u64) -> u64 {
    let (h1, depth1, bits1) = unpack_level_data(data1);
    let (h2, depth2, bits2) = unpack_level_data(data2);
    let h = mix_hash(salt, mix_hash(h1 as u64, h2 as u64)) as u32;
    pack_level_data(h, depth1.max(depth2) + 1, bits1 | bits2)
  }
  pub(crate) fn level_max_data(data1: u64, data2: u64) -> u64 {
    level_data_binary(2251, data1, data2)
  }
  pub(crate) fn level_imax_data(data1: u64, data2: u64) -> u64 {
    level_data_binary(2267, data1, data2)
  }
  pub(crate) const fn level_param_data(hash: u64) -> u64 {
    pack_level_data(mix_hash(2239, hash) as u32, 0, 2)
  }
  pub(crate) const fn level_mvar_data(hash: u64) -> u64 {
    pack_level_data(mix_hash(2237, hash) as u32, 0, 1)
  }

  pub(crate) const EXPR_BVAR: u8 = 0x07;
  pub(crate) const EXPR_FVAR: u8 = 0x08;
  pub(crate) const EXPR_MVAR: u8 = 0x09;
  pub(crate) const EXPR_SORT: u8 = 0x0a;
  pub(crate) const EXPR_LIT: u8 = 0x0b;
  pub(crate) const EXPR_MDATA: u8 = 0x0c;
  pub(crate) const EXPR_PROJ: u8 = 0x0d;
  pub(crate) const EXPR_LET: u8 = 0x0e;
  pub(crate) const EXPR_LET_END: u8 = 0x0f;
  pub(crate) const EXPR_LAMBDA: u8 = 0x18;
  pub(crate) const _EXPR_FORALL: u8 = 0x1c;
  pub(crate) const EXPR_FORALL_END: u8 = 0x1f;
  pub(crate) const EXPR_APP: u8 = 0x20; // 0x20 is not used, 0x20+n is n applications
  pub(crate) const EXPR_APP_END: u8 = EXPR_APP + 0x1f;
  pub(crate) const EXPR_CONST_APP: u8 = 0x40;
  pub(crate) const EXPR_CONST_APP_END: u8 = EXPR_CONST_APP + 0x1f;

  pub(crate) const fn pack_expr_data(h: u64, bvars: u32, depth: u8, bits: u8) -> u64 {
    (h as u32 as u64) | ((depth as u64) << 32) | ((bits as u64) << 40) | ((bvars as u64) << 44)
  }
  pub(crate) const fn unpack_expr_data(data: u64) -> (u64, u32, u8, u8) {
    (data as u32 as u64, (data >> 44) as u32, (data >> 32) as u8, ((data >> 40) & 15) as u8)
  }

  pub(crate) const fn expr_const_data(hash1: u64, hash2: u64, bits2: u8) -> u64 {
    pack_expr_data(mix_hash(5, mix_hash(hash1, hash2)), 0, 0, bits2 << 2)
  }
  pub(crate) const fn expr_bvar_data(n: u64) -> u64 {
    pack_expr_data(mix_hash(7, n), n as u32 + 1, 0, 0)
  }
  pub(crate) const fn expr_sort_data(data: u64) -> u64 {
    let (h, _, bits) = unpack_level_data(data);
    pack_expr_data(mix_hash(11, h as u64), 0, 0, bits << 2)
  }
  pub(crate) const fn expr_fvar_data(hash: u64) -> u64 {
    pack_expr_data(mix_hash(13, mix_hash(0, hash)), 0, 0, 1)
  }
  pub(crate) const fn expr_mvar_data(hash: u64) -> u64 {
    pack_expr_data(mix_hash(17, mix_hash(0, hash)), 0, 0, 2)
  }
  pub(crate) const fn expr_mdata_data(data: u64) -> u64 {
    let (h, bvars, depth, bits) = unpack_expr_data(data);
    let h = mix_hash(depth as u64 + 1, h);
    pack_expr_data(h, bvars, depth.saturating_add(1), bits)
  }
  pub(crate) const fn expr_proj_data(hash1: u64, num2: u64, data3: u64) -> u64 {
    let (h, bvars, depth, bits) = unpack_expr_data(data3);
    let h = mix_hash(depth as u64 + 1, mix_hash(hash1, mix_hash(num2, h)));
    pack_expr_data(h, bvars, depth.saturating_add(1), bits)
  }
  pub(crate) fn expr_app_data(data1: u64, data2: u64) -> u64 {
    let (_, bvars1, depth1, bits1) = unpack_expr_data(data1);
    let (_, bvars2, depth2, bits2) = unpack_expr_data(data2);
    let depth = depth1.max(depth2).saturating_add(1);
    pack_expr_data(mix_hash(data1, data2), bvars1.max(bvars2), depth, bits1 | bits2)
  }
  pub(crate) fn expr_binder_data(data1: u64, data2: u64) -> u64 {
    let (h1, bvars1, depth1, bits1) = unpack_expr_data(data1);
    let (h2, bvars2, depth2, bits2) = unpack_expr_data(data2);
    let depth = depth1.max(depth2);
    let h = mix_hash(depth as u64 + 1, mix_hash(h1, h2));
    let bvars = bvars1.max(bvars2.saturating_sub(1));
    pack_expr_data(h, bvars, depth.saturating_add(1), bits1 | bits2)
  }
  pub(crate) fn expr_let_data(data1: u64, data2: u64, data3: u64) -> u64 {
    let (h1, bvars1, depth1, bits1) = unpack_expr_data(data1);
    let (h2, bvars2, depth2, bits2) = unpack_expr_data(data2);
    let (h3, bvars3, depth3, bits3) = unpack_expr_data(data3);
    let depth = depth1.max(depth2).max(depth3);
    let h = mix_hash(depth as u64 + 1, mix_hash(h1, mix_hash(h2, h3)));
    let bvars = bvars1.max(bvars2).max(bvars3.saturating_sub(1));
    pack_expr_data(h, bvars, depth.saturating_add(1), bits1 | bits2 | bits3)
  }
  pub(crate) fn expr_lit_data(hash: u64) -> u64 { pack_expr_data(mix_hash(3, hash), 0, 0, 0) }
}

pub(crate) const fn pack_ctor(ctor: u8, num_fields: u8, sfields: u16) -> Option<u8> {
  // This encoding works for all constructors in the Lean library
  let lo = match (ctor, num_fields, sfields) {
    (_, 0, 0) => return None, // unused anyway because these are enum-like
    (..=12, ..=7, ..=1) => (num_fields << 1) | sfields as u8,
    _ => return None,
  };
  Some((ctor << 4) | lo)
}
pub(crate) const fn unpack_ctor(tag: u8) -> (u8, u16) {
  let n = tag & 15;
  (n >> 1, (n & 1) as u16)
}

const _: () = {
  let mut i = 0;
  while i < 0xff {
    let ctor = i >> 4;
    let (n, s) = unpack_ctor(i);
    if let Some(val) = pack_ctor(ctor, n, s) {
      assert!(val == i)
    }
    i += 1;
  }
};

pub const fn mix_hash(mut h: u64, mut k: u64) -> u64 {
  let m: u64 = 0xc6a4a7935bd1e995;
  let r: u64 = 47;
  k = k.wrapping_mul(m);
  k ^= k >> r;
  k ^= m;
  h ^= k;
  h = h.wrapping_mul(m);
  h
}

pub fn str_hash(bytes: &[u8]) -> u64 {
  const M: u64 = 0xc6a4a7935bd1e995;
  const R: u64 = 47;
  const SEED: u64 = 11;
  let mut h: u64 = SEED ^ (bytes.len() as u64).wrapping_mul(M);

  let (data, rest) = Ref::<_, [u64]>::from_prefix_with_elems(bytes, bytes.len() / 8).unwrap();
  for &(mut k) in &*data {
    k = k.wrapping_mul(M);
    k ^= k >> R;
    k = k.wrapping_mul(M);

    h ^= k;
    h = h.wrapping_mul(M);
  }

  if !rest.is_empty() {
    let mut buf = [0; 8];
    buf[..rest.len()].copy_from_slice(rest);
    h ^= u64::from_le_bytes(buf);
    h = h.wrapping_mul(M);
  }

  h ^= h >> R;
  h = h.wrapping_mul(M);
  h ^= h >> R;
  h
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LgzMode {
  Normal,
  Exprish,
}

fn get_num_hash(cfg: Config, buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 == 1 {
    Some(ptr >> 1)
  } else {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.tag == tag::MPZ {
      if cfg.use_gmp {
        let (_capacity, pos) = parse_u32(buf, pos);
        let (sign_size, pos) = parse_i32(buf, pos);
        let (_limbs_ptr, pos) = parse_u64(buf, pos);
        let (lo, _) = parse_u64(buf, pos);
        return Some(if sign_size < 0 { lo.wrapping_neg() } else { lo })
      } else {
        let (sign, pos) = parse_u64(buf, pos);
        let (_size, pos) = parse_u64(buf, pos);
        let (_limbs_ptr, pos) = parse_u64(buf, pos);
        let (lo, _) = parse_u64(buf, pos);
        return Some(if sign != 0 { lo.wrapping_neg() } else { lo })
      }
    }
    None
  }
}

#[cfg(feature = "debug")]
fn get_str(buf: &[u8], offset: u64, ptr: u64) -> &str {
  if ptr & 1 == 0 {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.tag == tag::STRING {
      let (size, pos) = parse_u64(buf, pos);
      return std::str::from_utf8(&buf[pos + 16..][..(size - 1) as usize]).unwrap()
    }
  }
  panic!()
}

#[cfg(feature = "debug")]
fn get_name<'a>(buf: &'a [u8], offset: u64, ptr: u64, out: &mut Vec<Result<&'a str, u64>>) {
  if ptr & 1 == 1 {
    if (ptr >> 1) == 0 {
      // Name.anonymous
      return
    }
  } else {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.tag == 1 && header.num_fields == 2 {
      let (ptr1, pos) = parse_u64(buf, pos);
      let (ptr2, _) = parse_u64(buf, pos);
      get_name(buf, offset, ptr1, out);
      out.push(Ok(get_str(buf, offset, ptr2)));
      return
    } else if header.tag == 2 && header.num_fields == 2 {
      let (ptr1, pos) = parse_u64(buf, pos);
      let (ptr2, _) = parse_u64(buf, pos);
      get_name(buf, offset, ptr1, out);
      out.push(Err(ptr2 >> 1));
      return
    }
  }
  panic!()
}

fn get_name_hash(buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 == 1 {
    if (ptr >> 1) == 0 {
      // Name.anonymous
      return Some(NAME_ANON_HASH)
    }
  } else {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.sfields() >= 1 {
      return Some(parse_u64(buf, pos + 8 * header.num_fields as usize).0)
    }
  }
  None
}

fn get_str_hash(buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 != 1 {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.tag == tag::STRING {
      let (size, pos) = parse_u64(buf, pos);
      return Some(str_hash(&buf[pos + 16..][..(size - 1) as usize]))
    }
  }
  None
}

fn get_level_data(buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 == 1 {
    if (ptr >> 1) == 0 {
      // Level.zero
      return Some(exprish::LEVEL_ZERO_DATA)
    }
  } else {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.sfields() >= 1 {
      return Some(parse_u64(buf, pos + 8 * header.num_fields as usize).0)
    }
  }
  None
}

fn get_expr_data(buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 == 0 {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.sfields() >= 1 {
      return Some(parse_u64(buf, pos + 8 * header.num_fields as usize).0)
    }
  }
  None
}
fn get_lit_hash(cfg: Config, buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 != 0 {
    return None
  }
  let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
  match header.tag {
    0 => get_num_hash(cfg, buf, offset, parse_u64(buf, pos).0),
    1 => get_str_hash(buf, offset, parse_u64(buf, pos).0),
    _ => None,
  }
}

fn get_list_level_data(buf: &[u8], offset: u64, mut ptr: u64) -> (u64, u8) {
  let (mut hash, mut bits) = (7, 0);
  while ptr & 1 == 0 {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.tag != 1 {
      break
    }
    if header.num_fields >= 2 {
      let (ptr1, pos) = parse_u64(buf, pos);
      let (ptr2, _) = parse_u64(buf, pos);
      let Some(data) = get_level_data(buf, offset, ptr1) else { break };
      let (h1, _, bits1) = exprish::unpack_level_data(data);
      hash = mix_hash(hash, h1 as u64);
      bits |= bits1;
      ptr = ptr2
    }
  }
  (hash, bits)
}

impl<W: Write> LgzWriter<'_, W> {
  fn write_i64(&mut self, mode: LgzMode, num: i64) {
    if (0..32).contains(&num) {
      self.write_op(mode, mode, UINT0 + num as u8);
    } else if let Ok(i) = i8::try_from(num) {
      self.write_op(mode, mode, INT1);
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else if let Ok(i) = i16::try_from(num) {
      self.write_op(mode, mode, INT2);
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else if let Ok(i) = i32::try_from(num) {
      self.write_op(mode, mode, INT4);
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else {
      self.write_op(mode, mode, INT8);
      self.file.write_all(&num.to_le_bytes()).unwrap();
    }
  }

  fn write_backref(&mut self, mode: LgzMode, num: u32) {
    if (0..8).contains(&num) {
      self.write_op(mode, mode, BACKREF0 + num as u8);
    } else if let Ok(i) = u8::try_from(num) {
      self.write_op(mode, mode, BACKREF1);
      self.file.write_u8(i).unwrap();
    } else if let Ok(i) = u16::try_from(num) {
      self.write_op(mode, mode, BACKREF2);
      self.file.write_u16::<LE>(i).unwrap();
    } else {
      self.write_op(mode, mode, BACKREF4);
      self.file.write_u32::<LE>(num).unwrap();
    }
  }

  fn is_reused(&mut self, ptr: u64) -> bool { self.refs[(ptr - self.offset) as usize >> 3] > 1 }

  fn write_op(&mut self, cur_mode: LgzMode, mode: LgzMode, op: u8) {
    #[cfg(feature = "debug")]
    match mode {
      LgzMode::Normal => STATS.normal[op as usize].fetch_add(1, Ordering::Relaxed),
      LgzMode::Exprish => STATS.exprish[op as usize].fetch_add(1, Ordering::Relaxed),
    };
    match (cur_mode, mode) {
      (LgzMode::Normal, LgzMode::Exprish) => self.file.write_all(&[EXPRISH, op]).unwrap(),
      (LgzMode::Exprish, LgzMode::Normal) => {
        #[cfg(feature = "debug")]
        panic!("non-expr in expr");
        #[cfg(not(feature = "debug"))]
        self.file.write_all(&[exprish::NORMAL, op]).unwrap()
      }
      _ => self.file.write_all(&[op]).unwrap(),
    }
  }

  fn parse_nary_app(
    &mut self, pos: usize, mode: LgzMode, ctor: u8, num_fields: u8, sfields: u16,
  ) -> Option<()> {
    let mut stack = vec![];
    let mut ptr = None;
    let mut state = (pos, ctor, num_fields, sfields);
    loop {
      match state {
        (pos, 4, 2, 1) => {
          // Expr.const
          let _p0 = pos - size_of::<ObjHeader>();
          let (ptr1, pos) = parse_u64(self.buf, pos);
          let (ptr2, pos) = parse_u64(self.buf, pos);
          let Some(hash1) = get_name_hash(self.buf, self.offset, ptr1) else { break };
          let (hash2, bits2) = get_list_level_data(self.buf, self.offset, ptr2);
          if exprish::expr_const_data(hash1, hash2, bits2) == parse_u64(self.buf, pos).0 {
            self.write_op(mode, LgzMode::Exprish, exprish::EXPR_CONST_APP + stack.len() as u8);
            self.write_obj(ptr1, LgzMode::Exprish);
            self.write_obj(ptr2, LgzMode::Normal);
            for ptr2 in stack.into_iter().rev() {
              self.write_obj(ptr2, LgzMode::Exprish);
            }
            return Some(())
          }
          #[cfg(feature = "debug")]
          panic!(
            "{_p0}: failed const data: {hash1:x} + {hash2:x} + {bits2:x} = {:x} != {:x}",
            exprish::expr_const_data(hash1, hash2, bits2),
            parse_u64(self.buf, pos).0
          )
        }
        (pos, 5, 2, 1) if stack.len() < 0x1f => {
          // Expr.app
          let _p0 = pos - size_of::<ObjHeader>();
          let (ptr1, pos) = parse_u64(self.buf, pos);
          let (ptr2, pos) = parse_u64(self.buf, pos);
          let Some(data1) = get_expr_data(self.buf, self.offset, ptr1) else { break };
          let Some(data2) = get_expr_data(self.buf, self.offset, ptr2) else { break };
          if exprish::expr_app_data(data1, data2) == parse_u64(self.buf, pos).0 {
            ptr = Some(ptr1);
            stack.push(ptr2);
            if ptr1 & 1 == 0 && !self.is_reused(ptr1) {
              let (header, pos) = parse::<ObjHeader>(self.buf, (ptr1 - self.offset) as usize);
              state = (pos, header.tag, header.num_fields, header.sfields());
              continue
            }
            break
          }
          #[cfg(feature = "debug")]
          panic!(
            "{_p0}: failed app data: {data1:x} + {data2:x} = {:x} != {:x}",
            exprish::expr_app_data(data1, data2),
            parse_u64(self.buf, pos).0,
          )
        }
        _ => {}
      }
      break
    }
    let ptr = ptr?;
    debug_assert!(!stack.is_empty());
    self.write_op(mode, LgzMode::Exprish, exprish::EXPR_APP + stack.len() as u8);
    self.write_obj(ptr, LgzMode::Exprish);
    for ptr2 in stack.into_iter().rev() {
      self.write_obj(ptr2, LgzMode::Exprish);
    }
    Some(())
  }

  fn try_write_exprish_ctor(
    &mut self, pos: usize, mode: LgzMode, ctor: u8, num_fields: u8, sfields: u16,
  ) -> Option<()> {
    // let res = (|| {
    match (ctor, num_fields, sfields) {
      // Name.anonymous, Level.zero not needed because it is a scalar
      (1, 2, 1) => {
        // Name.str
        let _p0 = pos - size_of::<ObjHeader>();
        let (ptr1, pos) = parse_u64(self.buf, pos);
        let (ptr2, pos) = parse_u64(self.buf, pos);
        let h1 = get_name_hash(self.buf, self.offset, ptr1)?;
        let h2 = get_str_hash(self.buf, self.offset, ptr2)?;
        if mix_hash(h1, h2) == parse_u64(self.buf, pos).0 {
          self.write_op(mode, LgzMode::Exprish, exprish::NAME_STR);
          self.write_obj(ptr1, LgzMode::Exprish);
          self.write_obj(ptr2, LgzMode::Normal);
          return Some(())
        }
        #[cfg(feature = "debug")]
        {
          let mut out = vec![];
          get_name(self.buf, self.offset, ptr1, &mut out);
          out.push(Ok(get_str(self.buf, self.offset, ptr2)));
          panic!(
            "{_p0}: failed name.str hash: {h1:x} + {h2:x} = {:x} != {:x} in {out:?}",
            mix_hash(h1, h2),
            parse_u64(self.buf, pos).0
          )
        }
      }
      (1, 1, 1) => {
        // Level.succ
        let _p0 = pos - size_of::<ObjHeader>();
        let (ptr, pos) = parse_u64(self.buf, pos);
        return (|| {
          if exprish::level_succ_data(get_level_data(self.buf, self.offset, ptr)?)
            == parse_u64(self.buf, pos).0
          {
            self.write_op(mode, LgzMode::Exprish, exprish::LEVEL_SUCC);
            self.write_obj(ptr, LgzMode::Exprish);
            return Some(())
          }
          None
        })()
        .or_else(|| {
          // Expr.fvar
          let hash = get_name_hash(self.buf, self.offset, ptr)?;
          if exprish::expr_fvar_data(hash) == parse_u64(self.buf, pos).0 {
            self.write_op(mode, LgzMode::Exprish, exprish::EXPR_FVAR);
            self.write_obj(ptr, LgzMode::Exprish);
            return Some(())
          }
          #[cfg(feature = "debug")]
          {
            let mut out = vec![];
            get_name(self.buf, self.offset, ptr, &mut out);
            panic!(
              "{_p0}: failed fvar hash: {hash:x} = {:x} != {:x} in {out:?}",
              exprish::expr_fvar_data(hash),
              parse_u64(self.buf, pos).0
            )
          }
          #[cfg(not(feature = "debug"))]
          None
        })
      }
      (2, 2, 1) => {
        // Level.max
        let (ptr1, pos) = parse_u64(self.buf, pos);
        let (ptr2, pos) = parse_u64(self.buf, pos);
        return (|| {
          if exprish::level_max_data(
            get_level_data(self.buf, self.offset, ptr1)?,
            get_level_data(self.buf, self.offset, ptr2)?,
          ) == parse_u64(self.buf, pos).0
          {
            self.write_op(mode, LgzMode::Exprish, exprish::LEVEL_MAX);
            self.write_obj(ptr1, LgzMode::Exprish);
            self.write_obj(ptr2, LgzMode::Exprish);
            return Some(())
          }
          None
        })()
        .or_else(|| {
          // Name.num
          let h1 = get_name_hash(self.buf, self.offset, ptr1)?;
          let h2 = if ptr2 & 1 == 1 { ptr2 >> 1 } else { 17 };
          if mix_hash(h1, h2) == parse_u64(self.buf, pos).0 {
            self.write_op(mode, LgzMode::Exprish, exprish::NAME_NUM);
            self.write_obj(ptr1, LgzMode::Exprish);
            self.write_obj(ptr2, LgzMode::Normal);
            return Some(())
          }
          None
        })
      }
      (3, 2, 1) => {
        // Level.imax
        let (ptr1, pos) = parse_u64(self.buf, pos);
        let (ptr2, pos) = parse_u64(self.buf, pos);
        if exprish::level_imax_data(
          get_level_data(self.buf, self.offset, ptr1)?,
          get_level_data(self.buf, self.offset, ptr2)?,
        ) == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::LEVEL_IMAX);
          self.write_obj(ptr1, LgzMode::Exprish);
          self.write_obj(ptr2, LgzMode::Exprish);
          return Some(())
        }
      }
      (4, 1, 1) => {
        // Level.param
        let (ptr, pos) = parse_u64(self.buf, pos);
        if exprish::level_param_data(get_name_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::LEVEL_PARAM);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }
      (5, 1, 1) => {
        // Level.mvar
        let (ptr, pos) = parse_u64(self.buf, pos);
        if exprish::level_mvar_data(get_name_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::LEVEL_MVAR);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }

      (0, 1, 1) => {
        // Expr.bvar
        let (ptr, pos) = parse_u64(self.buf, pos);
        if exprish::expr_bvar_data(get_num_hash(self.cfg, self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_BVAR);
          self.write_obj(ptr, LgzMode::Normal);
          return Some(())
        }
      }
      (2, 1, 1) => {
        // Expr.mvar
        let (ptr, pos) = parse_u64(self.buf, pos);
        if exprish::expr_mvar_data(get_name_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_MVAR);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }
      (3, 1, 1) => {
        // Expr.sort
        let (ptr, pos) = parse_u64(self.buf, pos);
        if exprish::expr_sort_data(get_level_data(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_SORT);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }
      // Expr.const, Expr.app
      (4 | 5, 2, 1) => return self.parse_nary_app(pos, mode, ctor, num_fields, sfields),
      (6 | 7, 3, 2) => {
        // Expr.lam, Expr.forallE
        let _p0 = pos - size_of::<ObjHeader>();
        let (name, pos) = parse_u64(self.buf, pos);
        let (ty, pos) = parse_u64(self.buf, pos);
        let (body, pos) = parse_u64(self.buf, pos);
        let (data, pos) = parse_u64(self.buf, pos);
        let (bi, _) = parse_u64(self.buf, pos);
        let data1 = get_expr_data(self.buf, self.offset, ty)?;
        let data2 = get_expr_data(self.buf, self.offset, body)?;
        if exprish::expr_binder_data(data1, data2) == data && bi & !3 == 0 {
          self.write_op(mode, LgzMode::Exprish, (ctor << 2) + bi as u8);
          self.write_obj(name, LgzMode::Exprish);
          self.write_obj(ty, LgzMode::Exprish);
          self.write_obj(body, LgzMode::Exprish);
          return Some(())
        }
        #[cfg(feature = "debug")]
        panic!(
          "{_p0}: failed binder data: {data1:x} + {data2:x} = {:x} != {data:x}",
          exprish::expr_binder_data(data1, data2),
        )
      }
      (8, 4, 2) => {
        // Expr.letE
        let (name, pos) = parse_u64(self.buf, pos);
        let (ty, pos) = parse_u64(self.buf, pos);
        let (value, pos) = parse_u64(self.buf, pos);
        let (body, pos) = parse_u64(self.buf, pos);
        let (data, pos) = parse_u64(self.buf, pos);
        let (non_dep, _) = parse_u64(self.buf, pos);
        if exprish::expr_let_data(
          get_expr_data(self.buf, self.offset, ty)?,
          get_expr_data(self.buf, self.offset, value)?,
          get_expr_data(self.buf, self.offset, body)?,
        ) == data
          && non_dep & !1 == 0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_LET + non_dep as u8);
          self.write_obj(name, LgzMode::Exprish);
          self.write_obj(ty, LgzMode::Exprish);
          self.write_obj(value, LgzMode::Exprish);
          self.write_obj(body, LgzMode::Exprish);
          return Some(())
        }
      }
      (9, 1, 1) => {
        // Expr.lit
        let _p0 = pos - size_of::<ObjHeader>();
        let (ptr, pos) = parse_u64(self.buf, pos);
        let hash = get_lit_hash(self.cfg, self.buf, self.offset, ptr)?;
        if exprish::expr_lit_data(hash) == parse_u64(self.buf, pos).0 {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_LIT);
          self.write_obj(ptr, LgzMode::Normal);
          return Some(())
        }
        #[cfg(feature = "debug")]
        panic!(
          "{_p0}: failed lit data: {hash:x} = {:x} != {:x}",
          exprish::expr_lit_data(hash),
          parse_u64(self.buf, pos).0
        )
      }
      (10, 2, 1) => {
        // Expr.mdata
        let (ptr1, pos) = parse_u64(self.buf, pos);
        let (ptr2, pos) = parse_u64(self.buf, pos);
        if exprish::expr_mdata_data(get_expr_data(self.buf, self.offset, ptr2)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_MDATA);
          self.write_obj(ptr1, LgzMode::Normal);
          self.write_obj(ptr2, LgzMode::Exprish);
          return Some(())
        }
      }
      (11, 3, 1) => {
        // Expr.proj
        let (ptr1, pos) = parse_u64(self.buf, pos);
        let (ptr2, pos) = parse_u64(self.buf, pos);
        let (ptr3, pos) = parse_u64(self.buf, pos);
        if exprish::expr_proj_data(
          get_name_hash(self.buf, self.offset, ptr1)?,
          get_num_hash(self.cfg, self.buf, self.offset, ptr2)?,
          get_expr_data(self.buf, self.offset, ptr3)?,
        ) == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, exprish::EXPR_PROJ);
          self.write_obj(ptr1, LgzMode::Exprish);
          self.write_obj(ptr2, LgzMode::Normal);
          self.write_obj(ptr3, LgzMode::Exprish);
          return Some(())
        }
      }
      _ => {}
    }
    None
    // })();
    // println!(
    //   "{:d$}{:x}: try_write_exprish_ctor {:?}[{d}]({:x}) {:x?}, {}, {} -> {res:?}",
    //   "",
    //   pos - size_of::<ObjHeader>(),
    //   mode,
    //   self.file.pos,
    //   ctor,
    //   num_fields,
    //   sfields,
    //   d = self.depth,
    // );
    // res
  }

  fn write_obj(&mut self, ptr: u64, mode: LgzMode) {
    if ptr & 1 == 1 {
      self.write_i64(mode, ptr as i64 >> 1);
      return
    }
    if let Some(&i) = self.backrefs.get(&ptr) {
      self.write_backref(mode, i);
      return
    }
    let save = self.is_reused(ptr);
    if save {
      self.write_op(mode, mode, SAVE);
    }
    let (header, mut pos) = parse::<ObjHeader>(self.buf, (ptr - self.offset) as usize);
    // println!(
    //   "{:d$}{:x}: write {:?}[{d}]({:x}) {:x?}, {}, {} (save = {save})",
    //   "",
    //   (ptr - self.offset) as usize + size_of::<HeaderV2>(),
    //   mode,
    //   self.file.pos,
    //   header.tag,
    //   header.num_fields,
    //   header.sfields(),
    //   d = self.depth,
    // );
    self.depth += 1;
    match header.tag {
      tag::ARRAY => {
        self.write_op(mode, LgzMode::Normal, ARRAY);
        let (size, pos) = parse_u64(self.buf, pos);
        self.write_i64(LgzMode::Normal, size.try_into().unwrap());
        let (_capacity, mut pos) = parse_u64(self.buf, pos);
        for _ in 0..size {
          let (ptr2, pos2) = parse_u64(self.buf, pos);
          self.write_obj(ptr2, LgzMode::Normal);
          pos = pos2;
        }
      }
      tag::SCALAR_ARRAY => {
        self.write_op(mode, LgzMode::Normal, SCALAR_ARRAY);
        let (size, pos) = parse_u64(self.buf, pos);
        self.write_i64(LgzMode::Normal, size.try_into().unwrap());
        let (_capacity, pos) = parse_u64(self.buf, pos);
        self.file.write_all(&self.buf[pos..][..size as usize]).unwrap();
      }
      tag::STRING => {
        self.write_op(mode, LgzMode::Normal, STRING);
        let (size, pos) = parse_u64(self.buf, pos);
        let (_capacity, pos) = parse_u64(self.buf, pos);
        let (_length, pos) = parse_u64(self.buf, pos);
        let s = &self.buf[pos..][..size as usize];
        let s2 = std::ffi::CStr::from_bytes_until_nul(s).unwrap().to_bytes_with_nul();
        // println!("string: {:?}", std::str::from_utf8(s2).unwrap());
        // Internal nulls are not supported, unclear whether lean allows this
        assert!(s2.len() == s.len());
        self.file.write_all(s2).unwrap();
      }
      tag::MPZ => {
        let (capacity, sign_size, pos) = if self.cfg.use_gmp {
          let (capacity, pos) = parse_u32(self.buf, pos);
          let (sign_size, pos) = parse_u32(self.buf, pos);
          let (_limbs_ptr, pos) = parse_u64(self.buf, pos);
          (capacity as usize, sign_size, pos)
        } else {
          let (sign, pos) = parse_u64(self.buf, pos);
          let (size, pos) = parse_u64(self.buf, pos);
          let (_limbs_ptr, pos) = parse_u64(self.buf, pos);
          let capacity = (size + 1) >> 1;
          assert!(capacity <= i32::MAX as u64);
          let sign_size = if sign != 0 { -(capacity as i32) as u32 } else { capacity as u32 };
          assert!(size & 1 == 0 || self.buf[pos + 8 * capacity as usize - 4..][..4] == [0; 4]);
          (capacity as usize, sign_size, pos)
        };
        self.write_op(mode, LgzMode::Normal, MPZ);
        self.write_i64(LgzMode::Normal, sign_size.into());
        self.file.write_all(&self.buf[pos..][..8 * capacity]).unwrap();
      }
      tag::THUNK | tag::TASK | tag::REF | tag::PROMISE => {
        let tag = match header.tag {
          tag::THUNK => THUNK,
          tag::TASK => TASK,
          tag::REF => REF,
          tag::PROMISE => PROMISE,
          _ => unreachable!(),
        };
        self.write_op(mode, LgzMode::Normal, tag);
        let (value, _) = parse_u64(self.buf, pos);
        self.write_obj(value, LgzMode::Normal);
      }
      tag::CLOSURE | tag::STRUCT_ARRAY | tag::EXTERNAL | tag::RESERVED => unreachable!(),
      ctor => {
        let sfields = (header.cs_sz.get() >> 3) - 1 - (header.num_fields as u16);
        if !ENABLE_EXPRISH
          || self.try_write_exprish_ctor(pos, mode, ctor, header.num_fields, sfields).is_none()
        {
          if let Some(packed) = pack_ctor(ctor, header.num_fields, sfields) {
            self.write_op(mode, LgzMode::Normal, packed);
          } else {
            self.write_op(mode, LgzMode::Normal, BIG_CTOR);
            self.file.write_all(&[ctor, header.num_fields]).unwrap();
            self.file.write_all(&sfields.to_le_bytes()).unwrap();
          }
          for _ in 0..header.num_fields {
            let (ptr2, pos2) = parse_u64(self.buf, pos);
            self.write_obj(ptr2, LgzMode::Normal);
            pos = pos2;
          }
          self.file.write_all(&self.buf[pos..][..8 * sfields as usize]).unwrap();
        }
      }
    }
    self.depth -= 1;
    // println!(
    //   "{:d$}{:x}: <- write[{d}]({:x}) {:x?}",
    //   "",
    //   (ptr - self.offset) as usize + size_of::<HeaderV2>(),
    //   self.file.pos,
    //   header.tag,
    //   d = self.depth,
    // );
    if save {
      self.backrefs.insert(ptr, self.backrefs.len().try_into().unwrap());
    }
  }
}

enum LgzVersion {
  V0,
  V1,
  V2,
}

pub fn decompress(mut infile: impl Read) -> Vec<u8> {
  let mut magic = [0u8; 4];
  infile.read_exact(&mut magic).unwrap();
  let version = match magic {
    MAGIC2 => LgzVersion::V2,
    MAGIC1 => LgzVersion::V1,
    MAGIC0 => LgzVersion::V0,
    _ => panic!("unrecognized LGZ version"),
  };
  let base = (infile.read_u32::<LE>().unwrap() as u64) << 16;
  let mut w = LgzDecompressor {
    cfg: Config { use_gmp: use_gmp(false) },
    buf: Vec::with_capacity(infile.read_u64::<LE>().unwrap() as usize),
    file: WithPosition { r: infile, pos: 8 },
    offset: base,
    // depth: 0,
    backrefs: Default::default(),
    stack: vec![],
    temp: vec![],
  };
  match version {
    LgzVersion::V0 => w.buf.write_all(b"oleanfile!!!!!!!").unwrap(),
    LgzVersion::V1 => {
      w.buf.extend_from_slice(b"olean");
      w.buf.push(1);
      let mut githash_in = [0; 40];
      w.file.read_exact(&mut githash_in).unwrap();
      if mac_v1_use_gmp(&githash_in) {
        w.cfg.use_gmp = use_gmp(true)
      }
      w.buf.extend_from_slice(&githash_in);
      w.buf.extend_from_slice(&[0; 2]);
    }
    LgzVersion::V2 => {
      w.buf.extend_from_slice(b"olean");
      w.buf.push(2);
      let mut githash_in = [0; 40];
      w.file.read_exact(&mut githash_in).unwrap();
      let flags = w.file.read_u8().unwrap();
      w.cfg.use_gmp = flags & 1 != 0;
      w.buf.push(w.cfg.use_gmp as u8);
      let mut lean_version_in = [0; 33];
      let major = w.file.read_u8().unwrap();
      if major == 0 {
        w.file.read_exact(&mut lean_version_in).unwrap();
      } else {
        let minor = w.file.read_u8().unwrap();
        let patch = w.file.read_u8().unwrap();
        let rc_m1 = w.file.read_u8().unwrap();
        let lean_version = LeanVersion::encode(major, minor, patch, rc_m1);
        lean_version_in[..lean_version.len()].copy_from_slice(lean_version.as_bytes());
      }
      w.buf.extend_from_slice(&lean_version_in);
      w.buf.extend_from_slice(&githash_in);
    }
  }
  w.buf.write_u64::<LE>(base).unwrap();
  let fixup_pos = w.buf.len();
  w.buf.write_u64::<LE>(0).unwrap(); // fixed below
  let root = w.write_obj();
  LE::write_u64(&mut w.buf[fixup_pos..], root);
  w.buf
}

pub struct WithPosition<R> {
  pub r: R,
  pub pos: usize,
}
impl<R: Read> Read for WithPosition<R> {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let n = self.r.read(buf)?;
    self.pos += n;
    Ok(n)
  }

  fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.r.read_exact(buf)?;
    self.pos += buf.len();
    Ok(())
  }
}

impl<W: Write> Write for WithPosition<W> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    let n = self.r.write(buf)?;
    self.pos += n;
    Ok(n)
  }

  fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
    self.r.write_all(buf)?;
    self.pos += buf.len();
    Ok(())
  }

  fn flush(&mut self) -> std::io::Result<()> { self.r.flush() }
}

struct LgzDecompressor<R> {
  cfg: Config,
  file: WithPosition<R>,
  buf: Vec<u8>,
  // depth: usize,
  offset: u64,
  backrefs: Vec<u64>,
  stack: Vec<U64<ZLE>>,
  temp: Vec<u8>,
}

impl<R: Read> LgzDecompressor<R> {
  fn pos(&self) -> u64 { self.offset + self.buf.len() as u64 }
  fn write_header(&mut self, tag: u8, cs_sz: u16, num_fields: u8) -> u64 {
    let pos = self.pos();
    // let start = self.buf.len();
    // println!(
    //   "{:d$}{start:x}: write_header[{d}]({:x}) {tag:x}, {cs_sz}, {num_fields}",
    //   "",
    //   self.file.pos,
    //   d = self.depth
    // );
    let header = ObjHeader { rc: 0.into(), cs_sz: cs_sz.into(), num_fields, tag };
    self.buf.write_all(header.as_bytes()).unwrap();
    pos
  }

  fn copy(&mut self, size: usize, size_padded: usize) {
    self.temp.resize(size, 0);
    self.file.read_exact(&mut self.temp).unwrap();
    self.temp.resize(size_padded, 0);
    self.buf.write_all(&self.temp).unwrap();
  }

  fn pop(&mut self, start: usize) {
    let buf = &self.stack[start..].as_bytes();
    // println!(
    //   "{:d$}{:x}: pop[{d}]({:x}) {:x?}",
    //   "",
    //   self.buf.len(),
    //   self.file.pos,
    //   self.stack[start..]
    //     .iter()
    //     .map(|res| res.get())
    //     .map(|res| if res & 1 == 0 { Ok(res - self.offset) } else { Err(res >> 1) })
    //     .collect::<Vec<_>>(),
    //   d = self.depth
    // );
    self.buf.write_all(buf).unwrap();
    self.stack.truncate(start);
  }

  fn read_i64(&mut self, tag: u8) -> i64 {
    match tag {
      UINT0..=UINT0_END => (tag - UINT0).into(),
      INT1 => self.file.read_i8().unwrap().into(),
      INT2 => self.file.read_i16::<LE>().unwrap().into(),
      INT4 => self.file.read_i32::<LE>().unwrap().into(),
      INT8 => self.file.read_i64::<LE>().unwrap(),
      _ => panic!("unexpected int"),
    }
  }

  fn read_backref(&mut self, tag: u8) -> u32 {
    match tag {
      BACKREF0..=BACKREF0_END => (tag - BACKREF0).into(),
      BACKREF1 => self.file.read_u8().unwrap().into(),
      BACKREF2 => self.file.read_u16::<LE>().unwrap().into(),
      BACKREF4 => self.file.read_u32::<LE>().unwrap(),
      _ => panic!("unexpected backref"),
    }
  }

  fn write_u32(&mut self, n: u32) { self.buf.write_u32::<LE>(n).unwrap(); }

  fn write_u64(&mut self, n: u64) { self.buf.write_u64::<LE>(n).unwrap(); }

  fn write_ctor_header(&mut self, ctor: u8, num_fields: u8, sfields: u16) -> u64 {
    self.write_header(ctor, (num_fields as u16 + 1 + sfields) << 3, num_fields)
  }

  fn write_ctor(&mut self, ctor: u8, num_fields: u8, sfields: u16) -> u64 {
    let start = self.stack.len();
    for _ in 0..num_fields {
      let value = self.write_obj();
      self.stack.push(value.into());
    }
    let pos = self.write_ctor_header(ctor, num_fields, sfields);
    self.pop(start);
    let size = (sfields << 3) as usize;
    self.copy(size, size);
    pos
  }

  fn write_str(&mut self) -> u64 {
    self.temp.clear();
    let mut len = 0;
    loop {
      let c = self.file.read_u8().unwrap();
      self.temp.push(c);
      if c == 0 {
        break
      }
      if c & 0xC0 != 0x80 {
        len += 1;
      }
    }
    let pos = self.write_header(tag::STRING, 1, 0);
    self.write_u64(self.temp.len() as u64);
    self.write_u64(self.temp.len() as u64);
    self.write_u64(len);
    // println!("{:d$}string: {:?}", "", std::str::from_utf8(&self.temp).unwrap(), d = self.depth);
    let (_, size2) = pad_to(self.temp.len(), 8);
    self.temp.resize(size2, 0);
    self.buf.append(&mut self.temp);
    pos
  }

  fn write_exprish(&mut self) -> u64 {
    let tag = self.file.read_u8().unwrap();
    // let start = self.buf.len();
    // println!(
    //   "{:d$}{start:x}: write_exprish[{d}]({:x}) {tag:x}",
    //   "",
    //   self.file.pos - 1,
    //   d = self.depth
    // );
    // self.depth += 1;
    #[allow(clippy::match_overlapping_arm)]
    let res = match tag {
      exprish::NORMAL => {
        // println!("!!!");
        self.write_obj()
      }

      exprish::NAME_STR => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_obj();
        let pos = self.write_ctor_header(1, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(mix_hash(
          get_name_hash(&self.buf, self.offset, ptr1).unwrap(),
          get_str_hash(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      exprish::NAME_NUM => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_obj();
        let pos = self.write_ctor_header(2, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(mix_hash(
          get_name_hash(&self.buf, self.offset, ptr1).unwrap(),
          if ptr2 & 1 == 1 { ptr2 >> 1 } else { 17 },
        ));
        pos
      }

      exprish::LEVEL_SUCC => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(1, 1, 1);
        self.write_u64(ptr);
        self.write_u64(exprish::level_succ_data(
          get_level_data(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      exprish::LEVEL_MAX => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_exprish();
        let pos = self.write_ctor_header(2, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(exprish::level_max_data(
          get_level_data(&self.buf, self.offset, ptr1).unwrap(),
          get_level_data(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      exprish::LEVEL_IMAX => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_exprish();
        let pos = self.write_ctor_header(3, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(exprish::level_imax_data(
          get_level_data(&self.buf, self.offset, ptr1).unwrap(),
          get_level_data(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      exprish::LEVEL_PARAM => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(4, 1, 1);
        self.write_u64(ptr);
        self.write_u64(exprish::level_param_data(
          get_name_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      exprish::LEVEL_MVAR => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(5, 1, 1);
        self.write_u64(ptr);
        self
          .write_u64(exprish::level_mvar_data(get_name_hash(&self.buf, self.offset, ptr).unwrap()));
        pos
      }

      exprish::EXPR_BVAR => {
        let ptr = self.write_obj();
        let pos = self.write_ctor_header(0, 1, 1);
        self.write_u64(ptr);
        self.write_u64(exprish::expr_bvar_data(
          get_num_hash(self.cfg, &self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      exprish::EXPR_FVAR => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(1, 1, 1);
        self.write_u64(ptr);
        self
          .write_u64(exprish::expr_fvar_data(get_name_hash(&self.buf, self.offset, ptr).unwrap()));
        pos
      }
      exprish::EXPR_MVAR => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(2, 1, 1);
        self.write_u64(ptr);
        self
          .write_u64(exprish::expr_mvar_data(get_name_hash(&self.buf, self.offset, ptr).unwrap()));
        pos
      }
      exprish::EXPR_SORT => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(3, 1, 1);
        self.write_u64(ptr);
        self
          .write_u64(exprish::expr_sort_data(get_level_data(&self.buf, self.offset, ptr).unwrap()));
        pos
      }
      exprish::EXPR_LIT => {
        let ptr = self.write_obj();
        let pos = self.write_ctor_header(9, 1, 1);
        self.write_u64(ptr);
        self.write_u64(exprish::expr_lit_data(
          get_lit_hash(self.cfg, &self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      exprish::EXPR_MDATA => {
        let ptr1 = self.write_obj();
        let ptr2 = self.write_exprish();
        let pos = self.write_ctor_header(10, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(exprish::expr_mdata_data(
          get_expr_data(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      exprish::EXPR_PROJ => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_obj();
        let ptr3 = self.write_exprish();
        let pos = self.write_ctor_header(11, 3, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(ptr3);
        self.write_u64(exprish::expr_proj_data(
          get_name_hash(&self.buf, self.offset, ptr1).unwrap(),
          get_num_hash(self.cfg, &self.buf, self.offset, ptr2).unwrap(),
          get_expr_data(&self.buf, self.offset, ptr3).unwrap(),
        ));
        pos
      }
      tag @ exprish::EXPR_LET..=exprish::EXPR_LET_END => {
        let name = self.write_exprish();
        let ty = self.write_exprish();
        let value = self.write_exprish();
        let body = self.write_exprish();
        let pos = self.write_ctor_header(8, 4, 2);
        self.write_u64(name);
        self.write_u64(ty);
        self.write_u64(value);
        self.write_u64(body);
        self.write_u64(exprish::expr_let_data(
          get_expr_data(&self.buf, self.offset, ty).unwrap(),
          get_expr_data(&self.buf, self.offset, value).unwrap(),
          get_expr_data(&self.buf, self.offset, body).unwrap(),
        ));
        self.write_u64((tag & 1) as u64);
        pos
      }
      tag @ exprish::EXPR_LAMBDA..=exprish::EXPR_FORALL_END => {
        let name = self.write_exprish();
        let ty = self.write_exprish();
        let body = self.write_exprish();
        let pos = self.write_ctor_header(tag >> 2, 3, 2);
        self.write_u64(name);
        self.write_u64(ty);
        self.write_u64(body);
        self.write_u64(exprish::expr_binder_data(
          get_expr_data(&self.buf, self.offset, ty).unwrap(),
          get_expr_data(&self.buf, self.offset, body).unwrap(),
        ));
        self.write_u64((tag & 3) as u64);
        pos
      }
      ..=exprish::EXPR_APP => unreachable!(),
      tag @ ..=exprish::EXPR_APP_END => {
        let mut pos = self.write_exprish();
        let mut data = get_expr_data(&self.buf, self.offset, pos).unwrap();
        for _ in 0..tag & 0x1f {
          let ptr2 = self.write_exprish();
          let pos2 = self.write_ctor_header(5, 2, 1);
          let data2 = get_expr_data(&self.buf, self.offset, ptr2).unwrap();
          data = exprish::expr_app_data(data, data2);
          // println!(
          //   "{:d$}{:x}: app[{d}]({:x}) {:x?}",
          //   "",
          //   self.buf.len(),
          //   self.file.pos,
          //   [pos, ptr2]
          //     .iter()
          //     .map(|res| if res & 1 == 0 { Ok(res - self.offset) } else { Err(res >> 1) })
          //     .collect::<Vec<_>>(),
          //   d = self.depth
          // );
          self.write_u64(pos);
          self.write_u64(ptr2);
          self.write_u64(data);
          pos = pos2
        }
        pos
      }
      tag @ exprish::EXPR_CONST_APP..=exprish::EXPR_CONST_APP_END => {
        let ptr1 = self.write_exprish();
        let hash1 = get_name_hash(&self.buf, self.offset, ptr1).unwrap();
        let ptr2 = self.write_obj();
        let (hash2, bits2) = get_list_level_data(&self.buf, self.offset, ptr2);
        let mut data = exprish::expr_const_data(hash1, hash2, bits2);
        let mut pos = self.write_ctor_header(4, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(data);
        for _ in 0..tag & 0x1f {
          let ptr2 = self.write_exprish();
          let pos2 = self.write_ctor_header(5, 2, 1);
          let data2 = get_expr_data(&self.buf, self.offset, ptr2).unwrap();
          data = exprish::expr_app_data(data, data2);
          // println!(
          //   "{:d$}{:x}: app[{d}]({:x}) {:x?}",
          //   "",
          //   self.buf.len(),
          //   self.file.pos,
          //   [pos, ptr2]
          //     .iter()
          //     .map(|res| if res & 1 == 0 { Ok(res - self.offset) } else { Err(res >> 1) })
          //     .collect::<Vec<_>>(),
          //   d = self.depth
          // );
          self.write_u64(pos);
          self.write_u64(ptr2);
          self.write_u64(data);
          pos = pos2
        }
        pos
      }

      tag @ (UINT0..=UINT0_END | INT1 | INT2 | INT4 | INT8) =>
        ((self.read_i64(tag) << 1) | 1) as u64,
      SAVE => {
        // self.depth -= 1;
        let pos = self.write_exprish();
        self.backrefs.push(pos);
        return pos
      }
      tag @ (BACKREF0..=BACKREF0_END | BACKREF1 | BACKREF2 | BACKREF4) => {
        let r = self.read_backref(tag);
        self.backrefs[r as usize]
      }
      _ => unreachable!(),
    };
    // self.depth -= 1;
    // println!(
    //   "{:d$}{start:x}: write_exprish[{d}]({:x}) {tag:x} -> {:x?} -> {:x}",
    //   "",
    //   self.file.pos,
    //   if res & 1 == 0 { Ok(res - self.offset) } else { Err(res >> 1) },
    //   self.buf.len(),
    //   d = self.depth,
    // );
    res
  }

  fn write_obj(&mut self) -> u64 {
    let tag = self.file.read_u8().unwrap();
    // let start = self.buf.len();
    // println!(
    //   "{:d$}{start:x}: write_obj[{d}]({:x}) {tag:x} = {:?}, {:?}",
    //   "",
    //   self.file.pos - 1,
    //   tag >> 4,
    //   unpack_ctor(tag),
    //   d = self.depth,
    // );
    // self.depth += 1;
    let pos;
    match tag {
      BIG_CTOR => {
        let ctor = self.file.read_u8().unwrap();
        let num_fields = self.file.read_u8().unwrap();
        let sfields = self.file.read_u16::<LE>().unwrap();
        pos = self.write_ctor(ctor, num_fields, sfields)
      }
      SAVE => {
        pos = self.write_obj();
        self.backrefs.push(pos);
      }
      tag @ (BACKREF0..=BACKREF0_END | BACKREF1 | BACKREF2 | BACKREF4) => {
        let r = self.read_backref(tag);
        pos = self.backrefs[r as usize]
      }
      ARRAY => {
        let tag = self.file.read_u8().unwrap();
        let size = self.read_i64(tag) as u64;
        let start = self.stack.len();
        for _ in 0..size {
          let value = self.write_obj();
          self.stack.push(value.into());
        }
        pos = self.write_header(tag::ARRAY, 1, 0);
        self.write_u64(size);
        self.write_u64(size);
        self.pop(start);
      }
      SCALAR_ARRAY => {
        let tag = self.file.read_u8().unwrap();
        let size = self.read_i64(tag) as u64;
        pos = self.write_header(tag::SCALAR_ARRAY, 1, 0);
        self.write_u64(size);
        self.write_u64(size);
        let (_, size2) = pad_to(size as usize, 8);
        self.copy(size as usize, size2);
      }
      STRING => pos = self.write_str(),
      MPZ => {
        let tag = self.file.read_u8().unwrap();
        let sign_size = self.read_i64(tag) as i32;
        let capacity = sign_size as u32 & 0x7FFFFFFF;
        let size = (capacity as usize) << 3;
        if self.cfg.use_gmp {
          pos = self.write_header(tag::MPZ, ((capacity + 3) as u16) << 3, 0);
          self.write_u32(capacity);
          self.write_u32(sign_size as u32);
          self.write_u64(self.pos() + 8);
          self.copy(size, size);
        } else {
          self.temp.resize(size, 0);
          self.file.read_exact(&mut self.temp).unwrap();
          let mut size2 = size >> 2;
          if self.temp[size - 4..] == [0; 4] {
            size2 -= 1
          }
          pos = self.write_header(tag::MPZ, ((size2 + 8) as u16) << 2, 0);
          self.write_u64((sign_size < 0) as u64);
          self.write_u64(size2 as u64);
          self.write_u64(self.pos() + 8);
          self.buf.write_all(&self.temp).unwrap();
        }
      }
      THUNK => {
        let value = self.write_obj();
        pos = self.write_header(tag::THUNK, 3 << 3, 0);
        self.write_u64(value);
        self.write_u64(0);
      }
      TASK => {
        let value = self.write_obj();
        pos = self.write_header(tag::TASK, 3 << 3, 0);
        self.write_u64(value);
        self.write_u64(0);
      }
      REF => {
        let value = self.write_obj();
        pos = self.write_header(tag::REF, 2 << 3, 0);
        self.write_u64(value);
      }
      PROMISE => {
        let value = self.write_obj();
        pos = self.write_header(tag::PROMISE, 2 << 3, 0);
        self.write_u64(value);
      }
      EXPRISH => pos = self.write_exprish(),
      tag @ ..=0xcf => {
        let ctor = tag >> 4;
        let (num_fields, sfields) = unpack_ctor(tag);
        pos = self.write_ctor(ctor, num_fields, sfields)
      }
      tag @ (UINT0..=UINT0_END | INT1 | INT2 | INT4 | INT8) =>
        pos = ((self.read_i64(tag) << 1) | 1) as u64,
    }
    // self.depth -= 1;
    // println!(
    //   "{:d$}{start:x}: write_obj[{d}]({:x}) {tag:x} -> {:x?} -> {:x}",
    //   "",
    //   self.file.pos,
    //   if pos & 1 == 0 { Ok(pos - self.offset) } else { Err(pos >> 1) },
    //   self.buf.len(),
    //   d = self.depth,
    // );
    pos
  }
}
