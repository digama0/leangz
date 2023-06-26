use byteorder::ByteOrder;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use hashbrown::HashMap;
use std::io::Read;
use std::io::Write;
use std::mem::size_of;
#[cfg(feature = "debug")]
use std::sync::atomic::{AtomicUsize, Ordering};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, LE, U16, U32, U64};

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

pub fn compress(olean: &[u8], mut outfile: impl Write) {
  let (header, rest) = LayoutVerified::<_, Header>::new_from_prefix(olean).expect("bad header");
  assert_eq!(&header.magic, b"oleanfile!!!!!!!");
  let base = header.base.get();
  let offset = base + size_of::<Header>() as u64;
  assert!(base & !((u32::MAX as u64) << 16) == 0);
  let mut refs = vec![0u8; rest.len() >> 3];
  let mut pos = 0;
  while pos < rest.len() {
    pos = on_subobjs(rest, pos, |ptr| {
      let b = &mut refs[(ptr - offset) as usize >> 3];
      *b = b.saturating_add(1);
    });
    pos = pad_to(pos, 8).1;
  }
  outfile.write_all(lgz::MAGIC).unwrap();
  let mut w = LgzWriter {
    file: WithPosition { r: outfile, pos: 4 },
    refs: &refs,
    offset,
    backrefs: Default::default(),
    buf: rest,
    depth: 0,
  };
  w.file.write_u32::<LE>((base >> 16) as u32).unwrap();
  w.file.write_u64::<LE>(olean.len() as u64).unwrap();
  w.write_obj(header.root.get(), LgzMode::Normal);
}

#[repr(C, align(8))]
#[derive(FromBytes, AsBytes)]
struct Header {
  magic: [u8; 16],
  base: U64<LE>,
  root: U64<LE>,
}

#[repr(C, align(8))]
#[derive(FromBytes, AsBytes)]
struct ObjHeader {
  rc: U32<LE>,
  cs_sz: U16<LE>,
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

fn parse<T: FromBytes>(buf: &[u8], pos: usize) -> (LayoutVerified<&[u8], T>, usize) {
  let t = LayoutVerified::<_, T>::new_from_prefix(&buf[pos..]).expect("bad header").0;
  (t, pos + size_of::<T>())
}

fn on_subobjs(buf: &[u8], pos0: usize, mut f: impl FnMut(u64)) -> usize {
  let (header, pos) = parse::<ObjHeader>(buf, pos0);
  debug_assert!(header.rc.get() == 0);
  match header.tag {
    tag::ARRAY => {
      let (size, pos) = parse_u64(buf, pos);
      let (capacity, pos) = parse_u64(buf, pos);
      debug_assert!(size == capacity && header.cs_sz.get() == 1 && header.num_fields == 0);
      on_array_subobjs(buf, size, pos, f)
    }
    tag::SCALAR_ARRAY => {
      let (size, pos) = parse_u64(buf, pos);
      let (capacity, pos) = parse_u64(buf, pos);
      debug_assert!(header.cs_sz.get() == 1 && size == capacity);
      pos + capacity as usize
    }
    tag::STRING => {
      let (size, pos) = parse_u64(buf, pos);
      let (capacity, pos) = parse_u64(buf, pos);
      debug_assert!(header.cs_sz.get() == 1 && size == capacity);
      let (_length, pos) = parse_u64(buf, pos);
      pos + capacity as usize
    }
    tag::MPZ => {
      let (capacity, pos) = parse_u32(buf, pos);
      let (size, pos) = parse_i32(buf, pos);
      debug_assert!(
        header.cs_sz.get() == ((capacity + 3) as u16) << 3
          && size.unsigned_abs() == capacity
          && capacity != 0
      );
      let (_limbs_ptr, pos) = parse_u64(buf, pos);
      pos + (8 * capacity) as usize
    }
    tag::THUNK | tag::TASK => {
      let (value, pos) = parse_u64(buf, pos);
      let (imp, pos) = parse_u64(buf, pos);
      debug_assert!(header.cs_sz.get() == 1 && imp == 0);
      on_leanobj!(f, value);
      pos
    }
    tag::REF => {
      let (value, pos) = parse_u64(buf, pos);
      debug_assert!(header.cs_sz.get() == 1);
      on_leanobj!(f, value);
      pos
    }
    tag::CLOSURE => panic!("closure"),
    tag::STRUCT_ARRAY => panic!("struct array"),
    tag::EXTERNAL => panic!("external"),
    tag::RESERVED => panic!("reserved"),
    _ctor => {
      let len_except_sfields = 8 + 8 * header.num_fields as usize;
      debug_assert!(
        len_except_sfields <= header.cs_sz.get() as usize && header.cs_sz.get() & 7 == 0
      );
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
  buf: &'a [u8],
  file: WithPosition<W>,
  depth: usize,
  offset: u64,
  refs: &'a [u8],
  backrefs: HashMap<u64, u32>,
}

mod lgz {
  pub(crate) const MAGIC: &[u8; 4] = b"LGZ!";

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

  // only valid in exprish mode
  pub(crate) mod exprish {
    use crate::mix_hash;

    pub(crate) const NORMAL: u8 = 0x20;

    pub(crate) const NAME_STR: u8 = 0x00;
    pub(crate) const NAME_NUM: u8 = 0x01;
    pub(crate) const NAME_ANON_HASH: u64 = 1723;

    pub(crate) const LEVEL_SUCC: u8 = 0x02;
    pub(crate) const LEVEL_MAX: u8 = 0x03;
    pub(crate) const LEVEL_IMAX: u8 = 0x04;
    pub(crate) const LEVEL_PARAM: u8 = 0x05;
    pub(crate) const LEVEL_MVAR: u8 = 0x06;
    pub(crate) const fn pack_level_data(h: u32, depth: u32, bits: u8) -> u64 {
      (h as u64) | (bits as u64) << 32 | (depth as u64) << 40
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
      (h as u32 as u64) | (depth as u64) << 32 | (bits as u64) << 40 | (bvars as u64) << 44
    }
    pub(crate) const fn unpack_expr_data(data: u64) -> (u64, u32, u8, u8) {
      (data as u32 as u64, (data >> 44) as u32, (data >> 32) as u8, (data >> 40 & 15) as u8)
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
      (..=12, ..=7, ..=1) => num_fields << 1 | sfields as u8,
      _ => return None,
    };
    Some(ctor << 4 | lo)
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
}

const fn mix_hash(mut h: u64, mut k: u64) -> u64 {
  let m: u64 = 0xc6a4a7935bd1e995;
  let r: u64 = 47;
  k = k.wrapping_mul(m);
  k ^= k >> r;
  k ^= m;
  h ^= k;
  h = h.wrapping_mul(m);
  h
}

fn str_hash(bytes: &[u8]) -> u64 {
  const M: u64 = 0xc6a4a7935bd1e995;
  const R: u64 = 47;
  const SEED: u64 = 11;
  let mut h: u64 = SEED ^ (bytes.len() as u64).wrapping_mul(M);

  let (data, rest) =
    LayoutVerified::<_, [u64]>::new_slice_from_prefix(bytes, bytes.len() / 8).unwrap();
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

fn get_num_hash(buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 == 1 {
    Some(ptr >> 1)
  } else {
    let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
    if header.tag == tag::MPZ {
      let (_capacity, pos) = parse_u32(buf, pos);
      let (sign_size, pos) = parse_i32(buf, pos);
      let (_limbs_ptr, pos) = parse_u64(buf, pos);
      let (lo, _) = parse_u64(buf, pos);
      return Some(if sign_size < 0 { lo.wrapping_neg() } else { lo })
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
      return Some(lgz::exprish::NAME_ANON_HASH)
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
      return Some(lgz::exprish::LEVEL_ZERO_DATA)
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
fn get_lit_hash(buf: &[u8], offset: u64, ptr: u64) -> Option<u64> {
  if ptr & 1 != 0 {
    return None
  }
  let (header, pos) = parse::<ObjHeader>(buf, (ptr - offset) as usize);
  match header.tag {
    0 => get_num_hash(buf, offset, parse_u64(buf, pos).0),
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
      let (h1, _, bits1) = lgz::exprish::unpack_level_data(data);
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
      self.write_op(mode, mode, lgz::UINT0 + num as u8);
    } else if let Ok(i) = i8::try_from(num) {
      self.write_op(mode, mode, lgz::INT1);
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else if let Ok(i) = i16::try_from(num) {
      self.write_op(mode, mode, lgz::INT2);
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else if let Ok(i) = i32::try_from(num) {
      self.write_op(mode, mode, lgz::INT4);
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else {
      self.write_op(mode, mode, lgz::INT8);
      self.file.write_all(&num.to_le_bytes()).unwrap();
    }
  }

  fn write_backref(&mut self, mode: LgzMode, num: u32) {
    if (0..8).contains(&num) {
      self.write_op(mode, mode, lgz::BACKREF0 + num as u8);
    } else if let Ok(i) = u8::try_from(num) {
      self.write_op(mode, mode, lgz::BACKREF1);
      self.file.write_u8(i).unwrap();
    } else if let Ok(i) = u16::try_from(num) {
      self.write_op(mode, mode, lgz::BACKREF2);
      self.file.write_u16::<LE>(i).unwrap();
    } else {
      self.write_op(mode, mode, lgz::BACKREF4);
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
      (LgzMode::Normal, LgzMode::Exprish) => self.file.write_all(&[lgz::EXPRISH, op]).unwrap(),
      (LgzMode::Exprish, LgzMode::Normal) => {
        #[cfg(feature = "debug")]
        panic!("non-expr in expr");
        #[cfg(not(feature = "debug"))]
        self.file.write_all(&[lgz::exprish::NORMAL, op]).unwrap()
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
          if lgz::exprish::expr_const_data(hash1, hash2, bits2) == parse_u64(self.buf, pos).0 {
            self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_CONST_APP + stack.len() as u8);
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
            lgz::exprish::expr_const_data(hash1, hash2, bits2),
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
          if lgz::exprish::expr_app_data(data1, data2) == parse_u64(self.buf, pos).0 {
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
            lgz::exprish::expr_app_data(data1, data2),
            parse_u64(self.buf, pos).0,
          )
        }
        _ => {}
      }
      break
    }
    let ptr = ptr?;
    debug_assert!(!stack.is_empty());
    self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_APP + stack.len() as u8);
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
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::NAME_STR);
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
          if lgz::exprish::level_succ_data(get_level_data(self.buf, self.offset, ptr)?)
            == parse_u64(self.buf, pos).0
          {
            self.write_op(mode, LgzMode::Exprish, lgz::exprish::LEVEL_SUCC);
            self.write_obj(ptr, LgzMode::Exprish);
            return Some(())
          }
          None
        })()
        .or_else(|| {
          // Expr.fvar
          let hash = get_name_hash(self.buf, self.offset, ptr)?;
          if lgz::exprish::expr_fvar_data(hash) == parse_u64(self.buf, pos).0 {
            self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_FVAR);
            self.write_obj(ptr, LgzMode::Exprish);
            return Some(())
          }
          #[cfg(feature = "debug")]
          {
            let mut out = vec![];
            get_name(self.buf, self.offset, ptr, &mut out);
            panic!(
              "{_p0}: failed fvar hash: {hash:x} = {:x} != {:x} in {out:?}",
              lgz::exprish::expr_fvar_data(hash),
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
          if lgz::exprish::level_max_data(
            get_level_data(self.buf, self.offset, ptr1)?,
            get_level_data(self.buf, self.offset, ptr2)?,
          ) == parse_u64(self.buf, pos).0
          {
            self.write_op(mode, LgzMode::Exprish, lgz::exprish::LEVEL_MAX);
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
            self.write_op(mode, LgzMode::Exprish, lgz::exprish::NAME_NUM);
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
        if lgz::exprish::level_imax_data(
          get_level_data(self.buf, self.offset, ptr1)?,
          get_level_data(self.buf, self.offset, ptr2)?,
        ) == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::LEVEL_IMAX);
          self.write_obj(ptr1, LgzMode::Exprish);
          self.write_obj(ptr2, LgzMode::Exprish);
          return Some(())
        }
      }
      (4, 1, 1) => {
        // Level.param
        let (ptr, pos) = parse_u64(self.buf, pos);
        if lgz::exprish::level_param_data(get_name_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::LEVEL_PARAM);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }
      (5, 1, 1) => {
        // Level.mvar
        let (ptr, pos) = parse_u64(self.buf, pos);
        if lgz::exprish::level_mvar_data(get_name_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::LEVEL_MVAR);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }

      (0, 1, 1) => {
        // Expr.bvar
        let (ptr, pos) = parse_u64(self.buf, pos);
        if lgz::exprish::expr_bvar_data(get_num_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_BVAR);
          self.write_obj(ptr, LgzMode::Normal);
          return Some(())
        }
      }
      (2, 1, 1) => {
        // Expr.mvar
        let (ptr, pos) = parse_u64(self.buf, pos);
        if lgz::exprish::expr_mvar_data(get_name_hash(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_MVAR);
          self.write_obj(ptr, LgzMode::Exprish);
          return Some(())
        }
      }
      (3, 1, 1) => {
        // Expr.sort
        let (ptr, pos) = parse_u64(self.buf, pos);
        if lgz::exprish::expr_sort_data(get_level_data(self.buf, self.offset, ptr)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_SORT);
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
        if lgz::exprish::expr_binder_data(data1, data2) == data && bi & !3 == 0 {
          self.write_op(mode, LgzMode::Exprish, (ctor << 2) + bi as u8);
          self.write_obj(name, LgzMode::Exprish);
          self.write_obj(ty, LgzMode::Exprish);
          self.write_obj(body, LgzMode::Exprish);
          return Some(())
        }
        #[cfg(feature = "debug")]
        panic!(
          "{_p0}: failed binder data: {data1:x} + {data2:x} = {:x} != {data:x}",
          lgz::exprish::expr_binder_data(data1, data2),
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
        if lgz::exprish::expr_let_data(
          get_expr_data(self.buf, self.offset, ty)?,
          get_expr_data(self.buf, self.offset, value)?,
          get_expr_data(self.buf, self.offset, body)?,
        ) == data
          && non_dep & !1 == 0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_LET + non_dep as u8);
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
        let hash = get_lit_hash(self.buf, self.offset, ptr)?;
        if lgz::exprish::expr_lit_data(hash) == parse_u64(self.buf, pos).0 {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_LIT);
          self.write_obj(ptr, LgzMode::Normal);
          return Some(())
        }
        #[cfg(feature = "debug")]
        panic!(
          "{_p0}: failed lit data: {hash:x} = {:x} != {:x}",
          lgz::exprish::expr_lit_data(hash),
          parse_u64(self.buf, pos).0
        )
      }
      (10, 2, 1) => {
        // Expr.mdata
        let (ptr1, pos) = parse_u64(self.buf, pos);
        let (ptr2, pos) = parse_u64(self.buf, pos);
        if lgz::exprish::expr_mdata_data(get_expr_data(self.buf, self.offset, ptr2)?)
          == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_MDATA);
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
        if lgz::exprish::expr_proj_data(
          get_name_hash(self.buf, self.offset, ptr1)?,
          get_num_hash(self.buf, self.offset, ptr2)?,
          get_expr_data(self.buf, self.offset, ptr3)?,
        ) == parse_u64(self.buf, pos).0
        {
          self.write_op(mode, LgzMode::Exprish, lgz::exprish::EXPR_PROJ);
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
      self.write_op(mode, mode, lgz::SAVE);
    }
    let (header, mut pos) = parse::<ObjHeader>(self.buf, (ptr - self.offset) as usize);
    // println!(
    //   "{:d$}{:x}: write {:?}[{d}]({:x}) {:x?}, {}, {} (save = {save})",
    //   "",
    //   (ptr - self.offset + 0x20) as usize,
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
        self.write_op(mode, LgzMode::Normal, lgz::ARRAY);
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
        self.write_op(mode, LgzMode::Normal, lgz::SCALAR_ARRAY);
        let (size, pos) = parse_u64(self.buf, pos);
        self.write_i64(LgzMode::Normal, size.try_into().unwrap());
        let (_capacity, pos) = parse_u64(self.buf, pos);
        self.file.write_all(&self.buf[pos..][..size as usize]).unwrap();
      }
      tag::STRING => {
        self.write_op(mode, LgzMode::Normal, lgz::STRING);
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
        self.write_op(mode, LgzMode::Normal, lgz::MPZ);
        let (capacity, pos) = parse_u32(self.buf, pos);
        let (sign_size, pos) = parse_u32(self.buf, pos);
        self.write_i64(LgzMode::Normal, sign_size.into());
        let (_limbs_ptr, pos) = parse_u64(self.buf, pos);
        self.file.write_all(&self.buf[pos..][..8 * capacity as usize]).unwrap();
      }
      tag::THUNK | tag::TASK | tag::REF => {
        self.write_op(mode, LgzMode::Normal, header.tag);
        let (value, _) = parse_u64(self.buf, pos);
        self.write_obj(value, LgzMode::Normal);
      }
      tag::CLOSURE | tag::STRUCT_ARRAY | tag::EXTERNAL | tag::RESERVED => unreachable!(),
      ctor => {
        let sfields = (header.cs_sz.get() >> 3) - 1 - (header.num_fields as u16);
        if !ENABLE_EXPRISH
          || self.try_write_exprish_ctor(pos, mode, ctor, header.num_fields, sfields).is_none()
        {
          if let Some(packed) = lgz::pack_ctor(ctor, header.num_fields, sfields) {
            self.write_op(mode, LgzMode::Normal, packed);
          } else {
            self.write_op(mode, LgzMode::Normal, lgz::BIG_CTOR);
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
    //   (ptr - self.offset + 0x20) as usize,
    //   self.file.pos,
    //   header.tag,
    //   d = self.depth,
    // );
    if save {
      self.backrefs.insert(ptr, self.backrefs.len().try_into().unwrap());
    }
  }
}

pub fn decompress(mut infile: impl Read) -> Vec<u8> {
  let mut magic = [0u8; 4];
  infile.read_exact(&mut magic).unwrap();
  assert_eq!(&magic, lgz::MAGIC);
  let base = (infile.read_u32::<LE>().unwrap() as u64) << 16;
  let mut out = Vec::with_capacity(infile.read_u64::<LE>().unwrap() as usize);
  out.write_all(b"oleanfile!!!!!!!").unwrap();
  out.write_u64::<LE>(base).unwrap();
  out.write_u64::<LE>(0).unwrap(); // fixed below
  let mut w = LgzDecompressor {
    file: WithPosition { r: infile, pos: 8 },
    buf: out,
    offset: base,
    // depth: 0,
    backrefs: Default::default(),
    stack: vec![],
    temp: vec![],
  };
  let root = w.write_obj();
  LE::write_u64(&mut w.buf[24..], root);
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
  file: WithPosition<R>,
  buf: Vec<u8>,
  // depth: usize,
  offset: u64,
  backrefs: Vec<u64>,
  stack: Vec<U64<LE>>,
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
      lgz::UINT0..=lgz::UINT0_END => (tag - lgz::UINT0).into(),
      lgz::INT1 => self.file.read_i8().unwrap().into(),
      lgz::INT2 => self.file.read_i16::<LE>().unwrap().into(),
      lgz::INT4 => self.file.read_i32::<LE>().unwrap().into(),
      lgz::INT8 => self.file.read_i64::<LE>().unwrap(),
      _ => panic!("unexpected int"),
    }
  }

  fn read_backref(&mut self, tag: u8) -> u32 {
    match tag {
      lgz::BACKREF0..=lgz::BACKREF0_END => (tag - lgz::BACKREF0).into(),
      lgz::BACKREF1 => self.file.read_u8().unwrap().into(),
      lgz::BACKREF2 => self.file.read_u16::<LE>().unwrap().into(),
      lgz::BACKREF4 => self.file.read_u32::<LE>().unwrap(),
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
      lgz::exprish::NORMAL => {
        // println!("!!!");
        self.write_obj()
      }

      lgz::exprish::NAME_STR => {
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
      lgz::exprish::NAME_NUM => {
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

      lgz::exprish::LEVEL_SUCC => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(1, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::level_succ_data(
          get_level_data(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::LEVEL_MAX => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_exprish();
        let pos = self.write_ctor_header(2, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(lgz::exprish::level_max_data(
          get_level_data(&self.buf, self.offset, ptr1).unwrap(),
          get_level_data(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      lgz::exprish::LEVEL_IMAX => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_exprish();
        let pos = self.write_ctor_header(3, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(lgz::exprish::level_imax_data(
          get_level_data(&self.buf, self.offset, ptr1).unwrap(),
          get_level_data(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      lgz::exprish::LEVEL_PARAM => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(4, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::level_param_data(
          get_name_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::LEVEL_MVAR => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(5, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::level_mvar_data(
          get_name_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }

      lgz::exprish::EXPR_BVAR => {
        let ptr = self.write_obj();
        let pos = self.write_ctor_header(0, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::expr_bvar_data(
          get_num_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::EXPR_FVAR => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(1, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::expr_fvar_data(
          get_name_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::EXPR_MVAR => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(2, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::expr_mvar_data(
          get_name_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::EXPR_SORT => {
        let ptr = self.write_exprish();
        let pos = self.write_ctor_header(3, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::expr_sort_data(
          get_level_data(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::EXPR_LIT => {
        let ptr = self.write_obj();
        let pos = self.write_ctor_header(9, 1, 1);
        self.write_u64(ptr);
        self.write_u64(lgz::exprish::expr_lit_data(
          get_lit_hash(&self.buf, self.offset, ptr).unwrap(),
        ));
        pos
      }
      lgz::exprish::EXPR_MDATA => {
        let ptr1 = self.write_obj();
        let ptr2 = self.write_exprish();
        let pos = self.write_ctor_header(10, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(lgz::exprish::expr_mdata_data(
          get_expr_data(&self.buf, self.offset, ptr2).unwrap(),
        ));
        pos
      }
      lgz::exprish::EXPR_PROJ => {
        let ptr1 = self.write_exprish();
        let ptr2 = self.write_obj();
        let ptr3 = self.write_exprish();
        let pos = self.write_ctor_header(11, 3, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(ptr3);
        self.write_u64(lgz::exprish::expr_proj_data(
          get_name_hash(&self.buf, self.offset, ptr1).unwrap(),
          get_num_hash(&self.buf, self.offset, ptr2).unwrap(),
          get_expr_data(&self.buf, self.offset, ptr3).unwrap(),
        ));
        pos
      }
      tag @ lgz::exprish::EXPR_LET..=lgz::exprish::EXPR_LET_END => {
        let name = self.write_exprish();
        let ty = self.write_exprish();
        let value = self.write_exprish();
        let body = self.write_exprish();
        let pos = self.write_ctor_header(8, 4, 2);
        self.write_u64(name);
        self.write_u64(ty);
        self.write_u64(value);
        self.write_u64(body);
        self.write_u64(lgz::exprish::expr_let_data(
          get_expr_data(&self.buf, self.offset, ty).unwrap(),
          get_expr_data(&self.buf, self.offset, value).unwrap(),
          get_expr_data(&self.buf, self.offset, body).unwrap(),
        ));
        self.write_u64((tag & 1) as u64);
        pos
      }
      tag @ lgz::exprish::EXPR_LAMBDA..=lgz::exprish::EXPR_FORALL_END => {
        let name = self.write_exprish();
        let ty = self.write_exprish();
        let body = self.write_exprish();
        let pos = self.write_ctor_header(tag >> 2, 3, 2);
        self.write_u64(name);
        self.write_u64(ty);
        self.write_u64(body);
        self.write_u64(lgz::exprish::expr_binder_data(
          get_expr_data(&self.buf, self.offset, ty).unwrap(),
          get_expr_data(&self.buf, self.offset, body).unwrap(),
        ));
        self.write_u64((tag & 3) as u64);
        pos
      }
      ..=lgz::exprish::EXPR_APP => unreachable!(),
      tag @ ..=lgz::exprish::EXPR_APP_END => {
        let mut pos = self.write_exprish();
        let mut data = get_expr_data(&self.buf, self.offset, pos).unwrap();
        for _ in 0..tag & 0x1f {
          let ptr2 = self.write_exprish();
          let pos2 = self.write_ctor_header(5, 2, 1);
          let data2 = get_expr_data(&self.buf, self.offset, ptr2).unwrap();
          data = lgz::exprish::expr_app_data(data, data2);
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
      tag @ lgz::exprish::EXPR_CONST_APP..=lgz::exprish::EXPR_CONST_APP_END => {
        let ptr1 = self.write_exprish();
        let hash1 = get_name_hash(&self.buf, self.offset, ptr1).unwrap();
        let ptr2 = self.write_obj();
        let (hash2, bits2) = get_list_level_data(&self.buf, self.offset, ptr2);
        let mut data = lgz::exprish::expr_const_data(hash1, hash2, bits2);
        let mut pos = self.write_ctor_header(4, 2, 1);
        self.write_u64(ptr1);
        self.write_u64(ptr2);
        self.write_u64(data);
        for _ in 0..tag & 0x1f {
          let ptr2 = self.write_exprish();
          let pos2 = self.write_ctor_header(5, 2, 1);
          let data2 = get_expr_data(&self.buf, self.offset, ptr2).unwrap();
          data = lgz::exprish::expr_app_data(data, data2);
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

      tag @ (lgz::UINT0..=lgz::UINT0_END | lgz::INT1 | lgz::INT2 | lgz::INT4 | lgz::INT8) =>
        (self.read_i64(tag) << 1 | 1) as u64,
      lgz::SAVE => {
        // self.depth -= 1;
        let pos = self.write_exprish();
        self.backrefs.push(pos);
        return pos
      }
      tag @ (lgz::BACKREF0..=lgz::BACKREF0_END | lgz::BACKREF1 | lgz::BACKREF2 | lgz::BACKREF4) => {
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
    //   lgz::unpack_ctor(tag),
    //   d = self.depth,
    // );
    // self.depth += 1;
    // let res =
    match tag {
      lgz::BIG_CTOR => {
        let ctor = self.file.read_u8().unwrap();
        let num_fields = self.file.read_u8().unwrap();
        let sfields = self.file.read_u16::<LE>().unwrap();
        self.write_ctor(ctor, num_fields, sfields)
      }
      lgz::SAVE => {
        // self.depth -= 1;
        let pos = self.write_obj();
        self.backrefs.push(pos);
        // return
        pos
      }
      tag @ (lgz::BACKREF0..=lgz::BACKREF0_END | lgz::BACKREF1 | lgz::BACKREF2 | lgz::BACKREF4) => {
        let r = self.read_backref(tag);
        self.backrefs[r as usize]
      }
      lgz::ARRAY => {
        let tag = self.file.read_u8().unwrap();
        let size = self.read_i64(tag) as u64;
        let start = self.stack.len();
        for _ in 0..size {
          let value = self.write_obj();
          self.stack.push(value.into());
        }
        let pos = self.write_header(tag::ARRAY, 1, 0);
        self.write_u64(size);
        self.write_u64(size);
        self.pop(start);
        pos
      }
      lgz::SCALAR_ARRAY => {
        let tag = self.file.read_u8().unwrap();
        let size = self.read_i64(tag) as u64;
        let pos = self.write_header(tag::SCALAR_ARRAY, 1, 0);
        self.write_u64(size);
        self.write_u64(size);
        let (_, size2) = pad_to(size as usize, 8);
        self.copy(size as usize, size2);
        pos
      }
      lgz::STRING => self.write_str(),
      lgz::MPZ => {
        let tag = self.file.read_u8().unwrap();
        let sign_size = self.read_i64(tag) as i32;
        let capacity = sign_size as u32 & 0x7FFFFFFF;
        let pos = self.write_header(tag::MPZ, ((capacity + 3) as u16) << 3, 0);
        self.write_u32(capacity);
        self.write_u32(sign_size as u32);
        self.write_u64(self.pos() + 8);
        let size = (capacity as usize) << 3;
        self.copy(size, size);
        pos
      }
      tag @ (lgz::THUNK | lgz::TASK) => {
        let value = self.write_obj();
        let pos = self.write_header(tag, 1, 0);
        self.write_u64(value);
        self.write_u64(0);
        pos
      }
      lgz::REF => {
        let value = self.write_obj();
        let pos = self.write_header(tag::TASK, 1, 0);
        self.write_u64(value);
        pos
      }
      lgz::EXPRISH => {
        // self.depth -= 1;
        // return
        self.write_exprish()
      }
      tag @ ..=0xcf => {
        const _X: (u8, (u8, u16)) = {
          let tag = 0x15;
          (tag >> 4, lgz::unpack_ctor(tag))
        };
        let ctor = tag >> 4;
        let (num_fields, sfields) = lgz::unpack_ctor(tag);
        self.write_ctor(ctor, num_fields, sfields)
      }
      tag @ (lgz::UINT0..=lgz::UINT0_END | lgz::INT1 | lgz::INT2 | lgz::INT4 | lgz::INT8) =>
        (self.read_i64(tag) << 1 | 1) as u64,
    }
    // self.depth -= 1;
    // println!(
    //   "{:d$}{start:x}: write_obj[{d}]({:x}) {tag:x} -> {:x?} -> {:x}",
    //   "",
    //   self.file.pos,
    //   if res & 1 == 0 { Ok(res - self.offset) } else { Err(res >> 1) },
    //   self.buf.len(),
    //   d = self.depth,
    // );
    // res
  }
}
