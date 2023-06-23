use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use hashbrown::HashMap;
use memmap2::Mmap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::mem::size_of;
use std::path::Path;
use zerocopy::{AsBytes, FromBytes, LayoutVerified, LE, U16, U32, U64};

fn main() {
  let help = || panic!("usage: leangz FILE.olean");
  let mut do_decompress = false;
  // let mut file = None;
  let mut outfile = None;
  let mut args = std::env::args();
  args.next();
  let mut args = args.peekable();
  while let Some(arg) = args.peek() {
    match &**arg {
      "-d" | "-x" => {
        do_decompress = true;
        args.next();
      }
      "-o" => {
        args.next();
        if outfile.replace(args.next().unwrap_or_else(help)).is_some() {
          help();
        }
      }
      _ => break,
    }
  }
  if do_decompress {
    for file in args {
      println!("{file}");
      let outfile = outfile.take().unwrap_or_else(|| format!("{file}.olean"));
      let file = File::open(file).unwrap();
      let outfile = File::create(outfile).unwrap();
      decompress(file, outfile);
    }
  } else {
    for file in args {
      println!("{file}");
      let outfile = outfile.take().unwrap_or_else(|| format!("{file}.lgz"));
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      compress(&mmap, outfile.as_ref());
    }
  }
}

fn compress(olean: &[u8], outfile: &Path) {
  let (header, rest) = LayoutVerified::<_, Header>::new_from_prefix(olean).expect("bad header");
  assert_eq!(&header.magic, b"oleanfile!!!!!!!");
  let offset = header.base.get() + size_of::<Header>() as u64;
  let mut refs = vec![0u8; rest.len() >> 3];
  let mut pos = 0;
  while pos < rest.len() {
    pos = on_subobjs(rest, pos, |ptr| {
      let b = &mut refs[(ptr - offset) as usize >> 3];
      *b = b.saturating_add(1);
    });
    pos = pad_to(pos, 8).1;
  }
  let mut file = BufWriter::new(File::create(outfile).unwrap());
  file.write_all(lgz::MAGIC).unwrap();
  let mut w = LgzWriter {
    file: flate2::write::GzEncoder::new(file, flate2::Compression::new(7)),
    refs: &refs,
    offset,
    backrefs: Default::default(),
    buf: rest,
  };
  w.file.write_all(header.base.as_bytes()).unwrap();
  w.write_obj(header.root.get());
  w.file.finish().unwrap();
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
      debug_assert!(buf[pos..][..size as usize].last() == Some(&0));
      pos + capacity as usize
    }
    tag::MPZ => {
      let (capacity, pos) = parse_u32(buf, pos);
      let (size, pos) = parse_u32(buf, pos);
      debug_assert!(
        header.cs_sz.get() == 24 + (8 * capacity as u16) && size & 0x7FFFFFFF == capacity
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
  file: W,
  offset: u64,
  refs: &'a [u8],
  backrefs: HashMap<u64, u32>,
}

mod lgz {
  pub(crate) const MAGIC: &[u8; 4] = b"LGZ!";
  pub(crate) const UINT0: u8 = 0xe0;
  pub(crate) const INT1: u8 = 0xf0;
  pub(crate) const INT2: u8 = 0xf1;
  pub(crate) const INT4: u8 = 0xf2;
  pub(crate) const INT8: u8 = 0xf3;
  pub(crate) const SAVE: u8 = 0xf5;
  pub(crate) const ARRAY: u8 = 0xf6;
  pub(crate) const BIG_CTOR: u8 = 0xf7;
  pub(crate) const SCALAR_ARRAY: u8 = 0xf8;
  pub(crate) const STRING: u8 = 0xf9;
  pub(crate) const MPZ: u8 = 0xfa;
  pub(crate) const THUNK: u8 = super::tag::THUNK;
  pub(crate) const TASK: u8 = super::tag::TASK;
  pub(crate) const REF: u8 = super::tag::REF;
  pub(crate) const BACKREF: u8 = 0xff;

  pub(crate) const fn pack_ctor(ctor: u8, num_fields: u8, sfields: u16) -> Option<u8> {
    // This encoding works for all constructors in the Lean library, and in particular
    // covers all the Expr constructors that make up the majority of oleans.
    if ctor >= 14 {
      return None
    }
    let lo = match (num_fields, sfields) {
      (4, 2) => 0,
      (3, 2) => 14,
      (0, 0) | (7, 0) => return None,
      (..=7, ..=1) => num_fields << 1 | sfields as u8,
      _ => return None,
    };
    Some(ctor << 4 | lo)
  }
  pub(crate) const fn unpack_ctor(tag: u8) -> (u8, u16) {
    match tag & 15 {
      0 => (4, 2),
      14 => (3, 2),
      n => (n >> 1, (n & 1) as u16),
    }
  }

  const _: () = {
    let mut i = 0;
    while i < 0xe0 {
      let ctor = i >> 4;
      let (n, s) = unpack_ctor(i);
      match pack_ctor(ctor, n, s) {
        Some(val) if val == i => {}
        _ => [][0],
      }
      i += 1;
    }
  };
}

impl<W: Write> LgzWriter<'_, W> {
  fn write_i64(&mut self, num: i64) {
    if (0..16).contains(&num) {
      self.file.write_all(&[lgz::UINT0 | num as u8]).unwrap();
    } else if let Ok(i) = i8::try_from(num) {
      self.file.write_all(&[lgz::INT1]).unwrap();
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else if let Ok(i) = i16::try_from(num) {
      self.file.write_all(&[lgz::INT2]).unwrap();
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else if let Ok(i) = i32::try_from(num) {
      self.file.write_all(&[lgz::INT4]).unwrap();
      self.file.write_all(&i.to_le_bytes()).unwrap();
    } else {
      self.file.write_all(&[lgz::INT8]).unwrap();
      self.file.write_all(&num.to_le_bytes()).unwrap();
    }
  }

  fn write_obj(&mut self, ptr: u64) {
    if ptr & 1 == 1 {
      self.write_i64(ptr as i64 >> 1);
      return
    }
    if let Some(&i) = self.backrefs.get(&ptr) {
      self.file.write_all(&[lgz::BACKREF]).unwrap();
      self.file.write_all(&i.to_le_bytes()).unwrap();
      return
    }
    let save = self.refs[(ptr - self.offset) as usize >> 3] > 1;
    if save {
      self.file.write_all(&[lgz::SAVE]).unwrap();
    }
    let (header, mut pos) = parse::<ObjHeader>(self.buf, (ptr - self.offset) as usize);
    match header.tag {
      tag::ARRAY => {
        self.file.write_all(&[lgz::ARRAY]).unwrap();
        let (size, pos) = parse_u64(self.buf, pos);
        self.write_i64(size.try_into().unwrap());
        let (_capacity, mut pos) = parse_u64(self.buf, pos);
        for _ in 0..size {
          let (ptr2, pos2) = parse_u64(self.buf, pos);
          self.write_obj(ptr2);
          pos = pos2;
        }
      }
      tag::SCALAR_ARRAY => {
        self.file.write_all(&[lgz::SCALAR_ARRAY]).unwrap();
        let (size, pos) = parse_u64(self.buf, pos);
        self.write_i64(size.try_into().unwrap());
        let (_capacity, pos) = parse_u64(self.buf, pos);
        self.file.write_all(&self.buf[pos..][..size as usize]).unwrap();
      }
      tag::STRING => {
        self.file.write_all(&[lgz::STRING]).unwrap();
        let (size, pos) = parse_u64(self.buf, pos);
        let (_capacity, pos) = parse_u64(self.buf, pos);
        let (_length, pos) = parse_u64(self.buf, pos);
        self.file.write_all(&self.buf[pos..][..size as usize]).unwrap();
      }
      tag::MPZ => {
        self.file.write_all(&[lgz::MPZ]).unwrap();
        let (capacity, pos) = parse_u32(self.buf, pos);
        let (sign_size, pos) = parse_u32(self.buf, pos);
        self.write_i64(sign_size.into());
        let (_limbs_ptr, pos) = parse_u64(self.buf, pos);
        self.file.write_all(&self.buf[pos..][..8 * capacity as usize]).unwrap();
      }
      tag::THUNK | tag::TASK | tag::REF => {
        self.file.write_all(&[header.tag]).unwrap();
        let (value, _) = parse_u64(self.buf, pos);
        self.write_obj(value);
      }
      tag::CLOSURE | tag::STRUCT_ARRAY | tag::EXTERNAL | tag::RESERVED => unreachable!(),
      ctor => {
        let sfields = (header.cs_sz.get() >> 3) - 1 - (header.num_fields as u16);
        if let Some(packed) = lgz::pack_ctor(ctor, header.num_fields, sfields) {
          self.file.write_all(&[packed]).unwrap();
        } else {
          self.file.write_all(&[lgz::BIG_CTOR, ctor, header.num_fields]).unwrap();
          self.file.write_all(&sfields.to_le_bytes()).unwrap();
        }
        for _ in 0..header.num_fields {
          let (ptr2, pos2) = parse_u64(self.buf, pos);
          self.write_obj(ptr2);
          pos = pos2;
        }
        self.file.write_all(&self.buf[pos..][..8 * sfields as usize]).unwrap();
      }
    }
    if save {
      self.backrefs.insert(ptr, self.backrefs.len().try_into().unwrap());
    }
  }
}

fn decompress(mut infile: File, outfile: File) {
  let mut magic = [0u8; 4];
  infile.read_exact(&mut magic).unwrap();
  assert_eq!(&magic, lgz::MAGIC);
  let mut file = flate2::read::GzDecoder::new(infile);
  let base = file.read_u64::<LE>().unwrap();
  let mut out = BufWriter::new(outfile);
  out.write_all(b"oleanfile!!!!!!!").unwrap();
  out.write_u64::<LE>(base).unwrap();
  out.write_u64::<LE>(0).unwrap(); // fixed below
  let mut w = LgzDecompressor {
    file,
    out,
    pos: base + 32,
    backrefs: Default::default(),
    stack: vec![],
    temp: vec![],
  };
  let root = w.write_obj();
  w.out.seek(SeekFrom::Start(24)).unwrap();
  w.out.write_u64::<LE>(root).unwrap();
  w.out.flush().unwrap();
}

struct LgzDecompressor<R> {
  file: R,
  out: BufWriter<File>,
  backrefs: Vec<u64>,
  pos: u64,
  stack: Vec<U64<LE>>,
  temp: Vec<u8>,
}

impl<R: Read> LgzDecompressor<R> {
  fn write_header(&mut self, tag: u8, cs_sz: u16, num_fields: u8) {
    let header = ObjHeader { rc: 0.into(), cs_sz: cs_sz.into(), num_fields, tag };
    self.out.write_all(header.as_bytes()).unwrap();
    self.pos += size_of::<ObjHeader>() as u64
  }

  fn copy(&mut self, size: usize, size_padded: usize) {
    self.temp.resize(size, 0);
    self.file.read_exact(&mut self.temp).unwrap();
    self.temp.resize(size_padded, 0);
    self.out.write_all(&self.temp).unwrap();
    self.pos += self.temp.len() as u64;
  }

  fn pop(&mut self, start: usize) {
    let buf = &self.stack[start..].as_bytes();
    self.out.write_all(buf).unwrap();
    self.pos += buf.len() as u64;
    self.stack.truncate(start);
  }

  fn read_i64(&mut self, tag: u8) -> i64 {
    if tag & !15 == lgz::UINT0 {
      return (tag & 15).into()
    }
    match tag {
      lgz::INT1 => self.file.read_i8().unwrap().into(),
      lgz::INT2 => self.file.read_i16::<LE>().unwrap().into(),
      lgz::INT4 => self.file.read_i32::<LE>().unwrap().into(),
      lgz::INT8 => self.file.read_i64::<LE>().unwrap(),
      _ => panic!("unexpected int"),
    }
  }

  fn write_u32(&mut self, n: u32) {
    self.out.write_u32::<LE>(n).unwrap();
    self.pos += 4;
  }

  fn write_u64(&mut self, n: u64) {
    self.out.write_u64::<LE>(n).unwrap();
    self.pos += 8;
  }

  fn write_ctor(&mut self, ctor: u8, num_fields: u8, sfields: u16) -> u64 {
    let start = self.stack.len();
    for _ in 0..num_fields {
      let value = self.write_obj();
      self.stack.push(value.into());
    }
    let pos = self.pos;
    self.write_header(ctor, (num_fields as u16 + 1 + sfields) << 3, num_fields);
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
      if c & 0x80 == 0 {
        len += 1;
      }
      self.temp.push(c);
      if c == 0 {
        break
      }
    }
    let pos = self.pos;
    self.write_header(tag::STRING, 1, 0);
    self.write_u64(self.temp.len() as u64);
    self.write_u64(self.temp.len() as u64);
    self.write_u64(len);
    let (_, size2) = pad_to(self.temp.len(), 8);
    self.temp.resize(size2, 0);
    self.out.write_all(&self.temp).unwrap();
    self.pos += self.temp.len() as u64;
    pos
  }

  fn write_obj(&mut self) -> u64 {
    match self.file.read_u8().unwrap() {
      tag @ ..=0xdf => {
        let ctor = tag >> 4;
        let (num_fields, sfields) = lgz::unpack_ctor(tag);
        self.write_ctor(ctor, num_fields, sfields)
      }
      tag @ ..=0xf3 => (self.read_i64(tag) << 1 | 1) as u64,
      lgz::BIG_CTOR => {
        let ctor = self.file.read_u8().unwrap();
        let num_fields = self.file.read_u8().unwrap();
        let sfields = self.file.read_u16::<LE>().unwrap();
        self.write_ctor(ctor, num_fields, sfields)
      }
      lgz::SAVE => {
        let pos = self.write_obj();
        self.backrefs.push(pos);
        pos
      }
      lgz::BACKREF => {
        let r = self.file.read_u32::<LE>().unwrap();
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
        let pos = self.pos;
        self.write_header(tag::ARRAY, 1, 0);
        self.write_u64(size);
        self.write_u64(size);
        self.pop(start);
        pos
      }
      lgz::SCALAR_ARRAY => {
        let tag = self.file.read_u8().unwrap();
        let size = self.read_i64(tag) as u64;
        let pos = self.pos;
        self.write_header(tag::SCALAR_ARRAY, 1, 0);
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
        let pos = self.pos;
        self.write_header(tag::MPZ, 1, 0);
        self.write_u32(capacity);
        self.write_u32(sign_size as u32);
        self.write_u64(self.pos + 8);
        let size = (capacity as usize) << 3;
        self.copy(size, size);
        pos
      }
      tag @ (lgz::THUNK | lgz::TASK) => {
        let value = self.write_obj();
        let pos = self.pos;
        self.write_header(tag, 1, 0);
        self.write_u64(value);
        self.write_u64(0);
        pos
      }
      lgz::REF => {
        let value = self.write_obj();
        let pos = self.pos;
        self.write_header(tag::TASK, 1, 0);
        self.write_u64(value);
        pos
      }
      244 | 254 => unreachable!(),
    }
  }
}
