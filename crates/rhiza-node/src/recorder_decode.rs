//! Allocation-aware decoding for recorder wire messages.
//!
//! This is deliberately a Serde adapter, rather than a post-decode validator:
//! collection capacity and owned strings are created by `Deserialize`.  The
//! adapter therefore hides collection size hints, accounts for an element
//! before its seed is invoked, and routes owned strings/bytes through the
//! borrowed deserializer entry points.  For a `Vec<T>`, the charge is three
//! times `size_of::<T>()` per item: with doubling growth, an allocator can hold
//! both the old and new element buffers, whose combined peak is below that
//! factor relative to the final logical length.  Strings and byte buffers are
//! charged by their decoded length before the target visitor creates them.
//!
//! This is a guard for slice-backed postcard and serde_json only. Postcard
//! supplies borrowed string/byte slices when routed through `str`/`bytes`.
//! Serde JSON supplies either a borrowed slice or a reference to its one
//! reusable escape scratch buffer; that buffer cannot exceed the bounded raw
//! JSON body, so it does not create an independent amplification path. A
//! streaming deserializer that creates an owned `String` before calling a
//! visitor cannot be made allocation-safe by a generic Serde wrapper.
//! Arbitrary third-party `Deserialize` implementations can also allocate
//! outside Serde's visitor APIs, so recorder ingress must keep using the
//! protocol's derived wire types.
//!
//! With wire cap `W` and heap budget `H`, postcard holds at most the input plus
//! `H` charged target allocations. JSON additionally has one reusable escape
//! scratch buffer; its own Vec growth can transiently hold old + new capacity
//! below `3W`. The default is `H = 4W`: it admits a near-capacity `Vec<u8>`
//! despite its conservative 3W growth accounting while bounding input + parser
//! + charged target memory to at most 8W (allocator/Rc bookkeeping excluded).

use std::{
    cell::{Cell, RefCell},
    error::Error,
    fmt, mem,
    rc::Rc,
};

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};

// These are the only production roots admitted by this core module.  The
// framed roots live in `recorder_tcp`; their matching sealed impls are deferred
// to the transport-integration change, so this J4a core cannot accidentally be
// used as a generic `T: Deserialize` allocation guard.
mod sealed {
    pub trait RecorderWireRoot {}
}

pub(crate) trait RecorderWireRoot: sealed::RecorderWireRoot + DeserializeOwned {}

macro_rules! recorder_wire_root {
    ($($body:ty),+ $(,)?) => {
        $(
            impl sealed::RecorderWireRoot for super::RecorderWire<$body> {}
            impl RecorderWireRoot for super::RecorderWire<$body> {}
        )+
    };
}

macro_rules! recorder_root {
    ($($root:ty),+ $(,)?) => {
        $(
            impl sealed::RecorderWireRoot for $root {}
            impl RecorderWireRoot for $root {}
        )+
    };
}

// HTTP request roots: identity, command/effect storage and fetch, both inspect
// requests, read fence, record, and proof installation. Response roots are
// listed as well because the same decoder is used for client responses.
// Audit result for these roots and their framed equivalents: owned variable
// heap shapes are Vec, String, PathBuf, and Option<Box<RecordSummary>>. There
// are no HashMap/BTreeMap/set/list/deque fields; `deserialize_map` is therefore
// fail-closed while JSON structs use the fixed-layout map path.
recorder_wire_root!(
    (),
    super::StoreCommandV2,
    super::FetchCommandV2,
    super::StageEffectChunkV3,
    super::FinalizeEffectBundleV3,
    super::FetchEffectManifestV3,
    super::FetchEffectChunkV3,
    super::InspectProofV2,
    rhiza_quepaxa::ReadFenceRequest,
    rhiza_quepaxa::RecordRequest,
    super::InstallProofV2,
    super::RecorderV2Result<()>,
    super::RecorderV2Result<String>,
    super::RecorderV2Result<Option<rhiza_core::StoredCommand>>,
    super::RecorderV2Result<Option<Vec<u8>>>,
    super::RecorderV2Result<rhiza_quepaxa::RecordSummary>,
    super::RecorderV2Result<Option<rhiza_quepaxa::DecisionProof>>,
    super::RecorderV2Result<Option<rhiza_quepaxa::RecordSummary>>,
    super::RecorderV2Result<rhiza_quepaxa::ReadFenceObservation>,
);

recorder_root!(
    super::recorder_tcp::Hello,
    super::recorder_tcp::HelloReply,
    super::recorder_tcp::RequestFrame,
    super::recorder_tcp::ResponseFrame,
    super::recorder_tcp::RecorderRequestBody,
    super::recorder_tcp::RecorderResponseBody,
);

/// Limits applied before an untrusted recorder payload becomes owned data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecorderDecodeLimits {
    pub(crate) max_wire_bytes: usize,
    pub(crate) max_decode_heap_bytes: usize,
    pub(crate) max_collection_items: usize,
}

impl RecorderDecodeLimits {
    pub(crate) const fn for_wire_bytes(max_wire_bytes: usize) -> Self {
        Self {
            max_wire_bytes,
            max_decode_heap_bytes: max_wire_bytes.saturating_mul(4),
            max_collection_items: max_wire_bytes,
        }
    }
}

/// Serde JSON has the same default recursion limit.  Keep an explicit bound so
/// postcard receives equivalent stack protection and a future JSON feature
/// cannot accidentally relax this boundary.
const MAX_NESTING_DEPTH: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecorderDecodeError {
    WireTooLarge { actual: usize, maximum: usize },
    Decode(String),
    TrailingBytes,
}

impl fmt::Display for RecorderDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WireTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "recorder wire body is {actual} bytes, over {maximum} byte limit"
                )
            }
            Self::Decode(message) => formatter.write_str(message),
            Self::TrailingBytes => formatter.write_str("recorder payload has trailing bytes"),
        }
    }
}

impl Error for RecorderDecodeError {}

#[derive(Debug)]
struct DecodeBudget {
    limits: RecorderDecodeLimits,
    heap_bytes: usize,
    collection_items: usize,
    nesting_depth: usize,
    failure: Option<&'static str>,
}

#[derive(Clone, Copy, Debug)]
struct ItemReservation {
    heap_bytes: usize,
}

impl DecodeBudget {
    fn new(limits: RecorderDecodeLimits) -> Self {
        Self {
            limits,
            heap_bytes: 0,
            collection_items: 0,
            nesting_depth: 0,
            failure: None,
        }
    }

    fn charge_heap(&mut self, bytes: usize) -> Result<(), &'static str> {
        let Some(next) = self.heap_bytes.checked_add(bytes) else {
            self.failure = Some("recorder decode heap budget overflow");
            return Err("recorder decode heap budget overflow");
        };
        if next > self.limits.max_decode_heap_bytes {
            self.failure = Some("recorder decode heap budget exceeded");
            return Err("recorder decode heap budget exceeded");
        }
        self.heap_bytes = next;
        Ok(())
    }

    fn reserve_item(
        &mut self,
        value_size: usize,
        startup_extra: usize,
    ) -> Result<ItemReservation, &'static str> {
        let Some(next) = self.collection_items.checked_add(1) else {
            self.failure = Some("recorder collection item budget overflow");
            return Err("recorder collection item budget overflow");
        };
        if next > self.limits.max_collection_items {
            self.failure = Some("recorder collection item budget exceeded");
            return Err("recorder collection item budget exceeded");
        }
        // A doubling Vec can hold old + new capacity during a non-in-place
        // growth.  This is below 3x the final logical element storage.
        // The bounded raw frame/body itself is intentionally not counted here.
        let Some(element_bytes) = value_size.checked_mul(3) else {
            self.failure = Some("recorder collection allocation budget overflow");
            return Err("recorder collection allocation budget overflow");
        };
        let Some(heap_bytes) = element_bytes.checked_add(startup_extra) else {
            self.failure = Some("recorder collection allocation budget overflow");
            return Err("recorder collection allocation budget overflow");
        };
        let Some(next_heap) = self.heap_bytes.checked_add(heap_bytes) else {
            self.failure = Some("recorder decode heap budget overflow");
            return Err("recorder decode heap budget overflow");
        };
        if next_heap > self.limits.max_decode_heap_bytes {
            self.failure = Some("recorder decode heap budget exceeded");
            return Err("recorder decode heap budget exceeded");
        }
        self.collection_items = next;
        self.heap_bytes = next_heap;
        Ok(ItemReservation { heap_bytes })
    }

    fn refund_item(&mut self, reservation: ItemReservation) {
        self.collection_items = self.collection_items.saturating_sub(1);
        self.heap_bytes = self.heap_bytes.saturating_sub(reservation.heap_bytes);
    }

    fn enter(&mut self) -> Result<(), &'static str> {
        let Some(next) = self.nesting_depth.checked_add(1) else {
            self.failure = Some("recorder decode nesting depth overflow");
            return Err("recorder decode nesting depth overflow");
        };
        if next > MAX_NESTING_DEPTH {
            self.failure = Some("recorder decode nesting depth exceeded");
            return Err("recorder decode nesting depth exceeded");
        }
        self.nesting_depth = next;
        Ok(())
    }

    fn leave(&mut self) {
        self.nesting_depth = self.nesting_depth.saturating_sub(1);
    }

    fn failure(&self) -> Option<&'static str> {
        self.failure
    }
}

type SharedBudget = Rc<RefCell<DecodeBudget>>;

fn charge_heap<E: de::Error>(budget: &SharedBudget, bytes: usize) -> Result<(), E> {
    budget.borrow_mut().charge_heap(bytes).map_err(E::custom)
}

fn reserve_item<E: de::Error>(
    budget: &SharedBudget,
    value_size: usize,
    startup_extra: usize,
) -> Result<ItemReservation, E> {
    budget
        .borrow_mut()
        .reserve_item(value_size, startup_extra)
        .map_err(E::custom)
}

fn reserve_optional_box<E: de::Error>(
    budget: &SharedBudget,
    visitor_value_size: usize,
) -> Result<(), E> {
    // Recorder roots contain exactly one boxed allocation shape:
    // Option<Box<RecordSummary>> in ReadFenceObservation.  Serde exposes Box
    // as an Option visitor but erases its inner type. Current recorder roots
    // have exactly one pointer-sized Option: Option<Box<RecordSummary>> in
    // ReadFenceObservation. This deliberately conservative shape check can
    // false-positive for a future pointer-sized Option, but the audit contains
    // no larger hidden Box allocation.
    if visitor_value_size == mem::size_of::<Option<Box<rhiza_quepaxa::RecordSummary>>>() {
        charge_heap::<E>(budget, mem::size_of::<rhiza_quepaxa::RecordSummary>())?;
    }
    Ok(())
}

fn rawvec_startup_extra(value_size: usize) -> usize {
    if value_size == 0 {
        return 0;
    }
    // RawVec's current minimum non-zero capacities are 8 elements for u8,
    // 4 for small elements, and 1 for larger elements. Charge only the part
    // not already covered by the first element's 3x growth reservation.
    let minimum_capacity: usize = if value_size == 1 {
        8
    } else if value_size <= 1024 {
        4
    } else {
        1
    };
    minimum_capacity
        .saturating_mul(value_size)
        .saturating_sub(value_size.saturating_mul(3))
}

fn with_depth<E: de::Error, T>(
    budget: &SharedBudget,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    budget.borrow_mut().enter().map_err(E::custom)?;
    let result = operation();
    budget.borrow_mut().leave();
    result
}

fn budget_or_decode_error<E: fmt::Debug>(budget: &SharedBudget, error: E) -> String {
    budget
        .borrow()
        .failure()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{error:?}"))
}

struct BoundedDeserializer<D> {
    inner: D,
    budget: SharedBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccessMode {
    Fixed,
    DynamicSeq,
    DynamicMap,
}

impl<D> BoundedDeserializer<D> {
    fn new(inner: D, budget: SharedBudget) -> Self {
        Self { inner, budget }
    }
}

struct BudgetVisitor<V> {
    inner: V,
    budget: SharedBudget,
    mode: AccessMode,
    charge_scalar_heap: bool,
}

impl<V> BudgetVisitor<V> {
    fn fixed(inner: V, budget: SharedBudget) -> Self {
        Self {
            inner,
            budget,
            mode: AccessMode::Fixed,
            charge_scalar_heap: true,
        }
    }

    fn dynamic_seq(inner: V, budget: SharedBudget) -> Self {
        Self {
            inner,
            budget,
            mode: AccessMode::DynamicSeq,
            charge_scalar_heap: true,
        }
    }

    fn dynamic_map(inner: V, budget: SharedBudget) -> Self {
        Self {
            inner,
            budget,
            mode: AccessMode::DynamicMap,
            charge_scalar_heap: true,
        }
    }

    fn identifier(inner: V, budget: SharedBudget) -> Self {
        Self {
            inner,
            budget,
            mode: AccessMode::Fixed,
            charge_scalar_heap: false,
        }
    }
}

impl<'de, V> Visitor<'de> for BudgetVisitor<V>
where
    V: Visitor<'de>,
{
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(formatter)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_bool(value)
    }

    fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_i8(value)
    }
    fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_i16(value)
    }
    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_i32(value)
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_i64(value)
    }
    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_i128(value)
    }
    fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_u8(value)
    }
    fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_u16(value)
    }
    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_u32(value)
    }
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_u64(value)
    }
    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_u128(value)
    }
    fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_f32(value)
    }
    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_f64(value)
    }
    fn visit_char<E>(self, value: char) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_char(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.charge_scalar_heap {
            charge_heap(&self.budget, value.len())?;
        }
        self.inner.visit_str(value)
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.charge_scalar_heap {
            charge_heap(&self.budget, value.len())?;
        }
        self.inner.visit_borrowed_str(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Kept for custom deserializers. Slice-backed postcard/JSON call one
        // of the borrowed forms above, so their allocation is charged first.
        if self.charge_scalar_heap {
            charge_heap(&self.budget, value.len())?;
        }
        self.inner.visit_string(value)
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.charge_scalar_heap {
            charge_heap(&self.budget, value.len())?;
        }
        self.inner.visit_bytes(value)
    }

    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.charge_scalar_heap {
            charge_heap(&self.budget, value.len())?;
        }
        self.inner.visit_borrowed_bytes(value)
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.charge_scalar_heap {
            charge_heap(&self.budget, value.len())?;
        }
        self.inner.visit_byte_buf(value)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_none()
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.inner.visit_unit()
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let budget = self.budget.clone();
        reserve_optional_box::<D::Error>(&budget, mem::size_of::<V::Value>())?;
        with_depth(&budget.clone(), || {
            self.inner
                .visit_some(BoundedDeserializer::new(deserializer, budget))
        })
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let budget = self.budget.clone();
        with_depth(&budget.clone(), || {
            self.inner
                .visit_newtype_struct(BoundedDeserializer::new(deserializer, budget))
        })
    }

    fn visit_seq<A>(self, sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let budget = self.budget.clone();
        with_depth(&budget.clone(), || match self.mode {
            AccessMode::Fixed => self.inner.visit_seq(FixedSeqAccess {
                inner: sequence,
                budget,
            }),
            AccessMode::DynamicSeq => self.inner.visit_seq(DynamicSeqAccess {
                inner: sequence,
                budget,
                started: Rc::new(Cell::new(false)),
            }),
            AccessMode::DynamicMap => Err(<A::Error as de::Error>::custom(
                "recorder dynamic map dispatched as a sequence",
            )),
        })
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        if self.mode == AccessMode::DynamicMap {
            return Err(<A::Error as de::Error>::custom(
                "dynamic maps are not part of recorder wire schemas",
            ));
        }
        let budget = self.budget.clone();
        with_depth(&budget.clone(), || {
            self.inner.visit_map(FixedMapAccess { inner: map, budget })
        })
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: EnumAccess<'de>,
    {
        let budget = self.budget.clone();
        with_depth(&budget.clone(), || {
            self.inner.visit_enum(BoundedEnumAccess {
                inner: data,
                budget,
            })
        })
    }
}

struct BoundedSeed<S> {
    inner: S,
    budget: SharedBudget,
    dynamic_item: bool,
    dynamic_started: Option<Rc<Cell<bool>>>,
}

impl<S> BoundedSeed<S> {
    fn item(inner: S, budget: SharedBudget, dynamic_started: Rc<Cell<bool>>) -> Self {
        Self {
            inner,
            budget,
            dynamic_item: true,
            dynamic_started: Some(dynamic_started),
        }
    }

    fn nested(inner: S, budget: SharedBudget) -> Self {
        Self {
            inner,
            budget,
            dynamic_item: false,
            dynamic_started: None,
        }
    }
}

impl<'de, S> DeserializeSeed<'de> for BoundedSeed<S>
where
    S: DeserializeSeed<'de>,
{
    type Value = S::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        if self.dynamic_item {
            // `next_element_seed` calls this only after it has determined that
            // an element exists. Reserve before that element's Deserialize can
            // allocate; an error refunds this speculative Vec capacity charge.
            let value_size = mem::size_of::<S::Value>();
            let startup_extra = self
                .dynamic_started
                .as_ref()
                .filter(|started| !started.get())
                .map_or(0, |_| rawvec_startup_extra(value_size));
            let reservation = reserve_item::<D::Error>(&self.budget, value_size, startup_extra)?;
            match self
                .inner
                .deserialize(BoundedDeserializer::new(deserializer, self.budget.clone()))
            {
                Ok(value) => {
                    if let Some(started) = self.dynamic_started {
                        started.set(true);
                    }
                    return Ok(value);
                }
                Err(error) => {
                    self.budget.borrow_mut().refund_item(reservation);
                    return Err(error);
                }
            }
        }
        self.inner
            .deserialize(BoundedDeserializer::new(deserializer, self.budget))
    }
}

struct DynamicSeqAccess<A> {
    inner: A,
    budget: SharedBudget,
    started: Rc<Cell<bool>>,
}

impl<'de, A> SeqAccess<'de> for DynamicSeqAccess<A>
where
    A: SeqAccess<'de>,
{
    type Error = A::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.inner.next_element_seed(BoundedSeed::item(
            seed,
            self.budget.clone(),
            self.started.clone(),
        ))
    }

    fn size_hint(&self) -> Option<usize> {
        // Never let VecVisitor or a map visitor preallocate from hostile wire
        // cardinality.  The per-element seed is charged before each push.
        None
    }
}

struct FixedSeqAccess<A> {
    inner: A,
    budget: SharedBudget,
}

impl<'de, A> SeqAccess<'de> for FixedSeqAccess<A>
where
    A: SeqAccess<'de>,
{
    type Error = A::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.inner
            .next_element_seed(BoundedSeed::nested(seed, self.budget.clone()))
    }

    fn size_hint(&self) -> Option<usize> {
        None
    }
}

struct FixedMapAccess<A> {
    inner: A,
    budget: SharedBudget,
}

impl<'de, A> MapAccess<'de> for FixedMapAccess<A>
where
    A: MapAccess<'de>,
{
    type Error = A::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        self.inner
            .next_key_seed(BoundedSeed::nested(seed, self.budget.clone()))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        self.inner
            .next_value_seed(BoundedSeed::nested(seed, self.budget.clone()))
    }

    fn size_hint(&self) -> Option<usize> {
        None
    }
}

struct BoundedEnumAccess<A> {
    inner: A,
    budget: SharedBudget,
}

impl<'de, A> EnumAccess<'de> for BoundedEnumAccess<A>
where
    A: EnumAccess<'de>,
{
    type Error = A::Error;
    type Variant = BoundedVariantAccess<A::Variant>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let (value, variant) = self
            .inner
            .variant_seed(BoundedSeed::nested(seed, self.budget.clone()))?;
        Ok((
            value,
            BoundedVariantAccess {
                inner: variant,
                budget: self.budget,
            },
        ))
    }
}

struct BoundedVariantAccess<A> {
    inner: A,
    budget: SharedBudget,
}

impl<'de, A> VariantAccess<'de> for BoundedVariantAccess<A>
where
    A: VariantAccess<'de>,
{
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.inner
            .newtype_variant_seed(BoundedSeed::nested(seed, self.budget))
    }

    fn tuple_variant<V>(self, length: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .tuple_variant(length, BudgetVisitor::fixed(visitor, self.budget))
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .struct_variant(fields, BudgetVisitor::fixed(visitor, self.budget))
    }
}

macro_rules! delegate_deserialize {
    ($($method:ident $(($($argument:ident : $argument_ty:ty),*))?);+ $(;)?) => {
        $(
            fn $method<V>(self, $($($argument: $argument_ty,)*)? visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                self.inner.$method($($($argument,)*)? BudgetVisitor::fixed(visitor, self.budget))
            }
        )+
    };
}

impl<'de, D> de::Deserializer<'de> for BoundedDeserializer<D>
where
    D: de::Deserializer<'de>,
{
    type Error = D::Error;

    delegate_deserialize! {
        deserialize_any;
        deserialize_bool;
        deserialize_i8;
        deserialize_i16;
        deserialize_i32;
        deserialize_i64;
        deserialize_i128;
        deserialize_u8;
        deserialize_u16;
        deserialize_u32;
        deserialize_u64;
        deserialize_u128;
        deserialize_f32;
        deserialize_f64;
        deserialize_char;
        deserialize_str;
        deserialize_bytes;
        deserialize_option;
        deserialize_unit;
        deserialize_ignored_any;
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_seq(BudgetVisitor::dynamic_seq(visitor, self.budget))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_map(BudgetVisitor::dynamic_map(visitor, self.budget))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_identifier(BudgetVisitor::identifier(visitor, self.budget))
    }

    // String and Vec<u8> call these owned entry points.  Both postcard's and
    // serde_json's slice deserializers can instead supply a borrowed/scratch
    // slice through `str`/`bytes`, so budget accounting happens first.
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_str(BudgetVisitor::fixed(visitor, self.budget))
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_bytes(BudgetVisitor::fixed(visitor, self.budget))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_unit_struct(name, BudgetVisitor::fixed(visitor, self.budget))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_newtype_struct(name, BudgetVisitor::fixed(visitor, self.budget))
    }

    fn deserialize_tuple<V>(self, length: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_tuple(length, BudgetVisitor::fixed(visitor, self.budget))
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner.deserialize_tuple_struct(
            name,
            length,
            BudgetVisitor::fixed(visitor, self.budget),
        )
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_struct(name, fields, BudgetVisitor::fixed(visitor, self.budget))
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.inner
            .deserialize_enum(name, variants, BudgetVisitor::fixed(visitor, self.budget))
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

/// Decode one complete postcard message under `limits`.
pub(crate) fn decode_postcard_exact_bounded<T>(
    bytes: &[u8],
    limits: RecorderDecodeLimits,
) -> Result<T, RecorderDecodeError>
where
    T: RecorderWireRoot,
{
    decode_postcard_inner(bytes, limits)
}

fn decode_postcard_inner<T>(
    bytes: &[u8],
    limits: RecorderDecodeLimits,
) -> Result<T, RecorderDecodeError>
where
    T: DeserializeOwned,
{
    if bytes.len() > limits.max_wire_bytes {
        return Err(RecorderDecodeError::WireTooLarge {
            actual: bytes.len(),
            maximum: limits.max_wire_bytes,
        });
    }
    let budget = Rc::new(RefCell::new(DecodeBudget::new(limits)));
    let mut deserializer = postcard::Deserializer::from_bytes(bytes);
    let value = T::deserialize(BoundedDeserializer::new(&mut deserializer, budget.clone()))
        .map_err(|error| RecorderDecodeError::Decode(budget_or_decode_error(&budget, error)))?;
    let remainder = deserializer
        .finalize()
        .map_err(|error| RecorderDecodeError::Decode(format!("{error:?}")))?;
    if remainder.is_empty() {
        Ok(value)
    } else {
        Err(RecorderDecodeError::TrailingBytes)
    }
}

/// Decode one complete JSON document under `limits`.
pub(crate) fn decode_json_exact_bounded<T>(
    bytes: &[u8],
    limits: RecorderDecodeLimits,
) -> Result<T, RecorderDecodeError>
where
    T: RecorderWireRoot,
{
    decode_json_inner(bytes, limits)
}

fn decode_json_inner<T>(
    bytes: &[u8],
    limits: RecorderDecodeLimits,
) -> Result<T, RecorderDecodeError>
where
    T: DeserializeOwned,
{
    if bytes.len() > limits.max_wire_bytes {
        return Err(RecorderDecodeError::WireTooLarge {
            actual: bytes.len(),
            maximum: limits.max_wire_bytes,
        });
    }
    let budget = Rc::new(RefCell::new(DecodeBudget::new(limits)));
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = T::deserialize(BoundedDeserializer::new(&mut deserializer, budget.clone()))
        .map_err(|error| RecorderDecodeError::Decode(budget_or_decode_error(&budget, error)))?;
    deserializer
        .end()
        .map_err(|error| RecorderDecodeError::Decode(format!("{error:?}")))?;
    Ok(value)
}

#[cfg(test)]
fn decode_postcard_for_test<T>(
    bytes: &[u8],
    limits: RecorderDecodeLimits,
) -> Result<T, RecorderDecodeError>
where
    T: DeserializeOwned,
{
    decode_postcard_inner(bytes, limits)
}

#[cfg(test)]
fn decode_json_for_test<T>(
    bytes: &[u8],
    limits: RecorderDecodeLimits,
) -> Result<T, RecorderDecodeError>
where
    T: DeserializeOwned,
{
    decode_json_inner(bytes, limits)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_json_exact_bounded, decode_json_for_test, decode_postcard_exact_bounded,
        decode_postcard_for_test, BoundedDeserializer, DecodeBudget, RecorderDecodeError,
        RecorderDecodeLimits,
    };
    use rhiza_core::LogHash;
    use rhiza_quepaxa::{
        DecisionProof, Proposal, ProposalPriority, RecordSummary, RecorderSummary,
    };
    use serde::{de::Visitor, Deserialize, Serialize};
    use std::{cell::RefCell, rc::Rc};

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    enum Choice {
        Unit,
        Struct { values: Vec<u16> },
    }

    #[derive(Debug, Deserialize)]
    struct Empty {}

    struct SizeHintChecked;

    #[derive(Serialize)]
    #[serde(tag = "status", content = "body")]
    enum LegacyRecorderV2Result<T> {
        Ok(T),
        #[allow(
            dead_code,
            reason = "the unconstructed legacy discriminant preserves the sealed postcard Error tag"
        )]
        Rejected(rhiza_quepaxa::RejectReason),
        Error(crate::RecorderWireError),
    }

    impl<'de> Deserialize<'de> for SizeHintChecked {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct CheckVisitor;

            impl<'de> Visitor<'de> for CheckVisitor {
                type Value = SizeHintChecked;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("a sequence")
                }

                fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::SeqAccess<'de>,
                {
                    assert_eq!(sequence.size_hint(), None);
                    while sequence.next_element::<u8>()?.is_some() {
                        assert_eq!(sequence.size_hint(), None);
                    }
                    Ok(SizeHintChecked)
                }
            }

            deserializer.deserialize_seq(CheckVisitor)
        }
    }

    fn growth_limits(bytes: &[u8]) -> RecorderDecodeLimits {
        RecorderDecodeLimits {
            max_wire_bytes: bytes.len(),
            max_decode_heap_bytes: bytes.len().saturating_mul(3),
            max_collection_items: bytes.len(),
        }
    }

    #[test]
    fn actual_http_recorder_wire_root_round_trips_under_default_limits() {
        let value = crate::RecorderWire {
            version: crate::RECORDER_WIRE_VERSION,
            remaining_deadline_ms: 1,
            body: crate::InspectProofV2 { slot: 7 },
        };
        let postcard = postcard::to_allocvec(&value).unwrap();
        let decoded: crate::RecorderWire<crate::InspectProofV2> = decode_postcard_exact_bounded(
            &postcard,
            RecorderDecodeLimits::for_wire_bytes(postcard.len()),
        )
        .unwrap();
        assert_eq!(decoded.version, value.version);
        assert_eq!(decoded.remaining_deadline_ms, value.remaining_deadline_ms);
        assert_eq!(decoded.body.slot, value.body.slot);
        let json = serde_json::to_vec(&value).unwrap();
        let decoded: crate::RecorderWire<crate::InspectProofV2> =
            decode_json_exact_bounded(&json, RecorderDecodeLimits::for_wire_bytes(json.len()))
                .unwrap();
        assert_eq!(decoded.version, value.version);
        assert_eq!(decoded.remaining_deadline_ms, value.remaining_deadline_ms);
        assert_eq!(decoded.body.slot, value.body.slot);
    }

    #[test]
    fn recorder_v2_result_manual_decode_keeps_canonical_json_and_postcard_bytes() {
        let ok = crate::RecorderV2Result::Ok(7_u64);
        let legacy_ok = LegacyRecorderV2Result::Ok(7_u64);
        let ok_json = serde_json::to_vec(&ok).unwrap();
        assert_eq!(ok_json, serde_json::to_vec(&legacy_ok).unwrap());
        assert_eq!(ok_json, br#"{"status":"Ok","body":7}"#);
        let ok_postcard = postcard::to_allocvec(&ok).unwrap();
        assert_eq!(ok_postcard, postcard::to_allocvec(&legacy_ok).unwrap());
        let decoded = decode_postcard_for_test::<crate::RecorderV2Result<u64>>(
            &ok_postcard,
            RecorderDecodeLimits::for_wire_bytes(ok_postcard.len()),
        );
        assert!(
            matches!(decoded, Ok(crate::RecorderV2Result::Ok(7))),
            "bytes={ok_postcard:?} decoded={decoded:?}"
        );
        assert!(matches!(
            serde_json::from_slice::<crate::RecorderV2Result<u64>>(&ok_json),
            Ok(crate::RecorderV2Result::Ok(7))
        ));

        let error = crate::RecorderWireError {
            code: crate::RecorderWireErrorCode::Decode,
            message: "bad".into(),
            detail: None,
        };
        let current_error = crate::RecorderV2Result::<u64>::Error(error.clone());
        let legacy_error = LegacyRecorderV2Result::<u64>::Error(error);
        let error_json = serde_json::to_vec(&current_error).unwrap();
        assert_eq!(error_json, serde_json::to_vec(&legacy_error).unwrap());
        assert!(matches!(
            serde_json::from_slice::<crate::RecorderV2Result<u64>>(&error_json),
            Ok(crate::RecorderV2Result::Error(_))
        ));
        let error_postcard = postcard::to_allocvec(&current_error).unwrap();
        assert_eq!(
            error_postcard,
            postcard::to_allocvec(&legacy_error).unwrap()
        );
        assert!(matches!(
            decode_postcard_for_test::<crate::RecorderV2Result<u64>>(
                &error_postcard,
                RecorderDecodeLimits::for_wire_bytes(error_postcard.len())
            ),
            Ok(crate::RecorderV2Result::Error(_))
        ));
    }

    #[test]
    fn sealed_production_response_root_uses_the_manual_result_decoder() {
        type ResponseRoot =
            crate::RecorderWire<crate::RecorderV2Result<Option<rhiza_core::StoredCommand>>>;
        let response = ResponseRoot {
            version: crate::RECORDER_WIRE_VERSION,
            remaining_deadline_ms: 1,
            body: crate::RecorderV2Result::Ok(None),
        };
        let postcard = postcard::to_allocvec(&response).unwrap();
        assert!(matches!(
            decode_postcard_exact_bounded::<ResponseRoot>(
                &postcard,
                RecorderDecodeLimits::for_wire_bytes(postcard.len())
            ),
            Ok(ResponseRoot {
                body: crate::RecorderV2Result::Ok(None),
                ..
            })
        ));
        let json = serde_json::to_vec(&response).unwrap();
        assert!(matches!(
            decode_json_exact_bounded::<ResponseRoot>(
                &json,
                RecorderDecodeLimits::for_wire_bytes(json.len())
            ),
            Ok(ResponseRoot {
                body: crate::RecorderV2Result::Ok(None),
                ..
            })
        ));
    }

    #[test]
    fn recorder_v2_json_requires_status_before_body_without_decoding_the_body() {
        type ResponseRoot =
            crate::RecorderWire<crate::RecorderV2Result<Option<rhiza_core::StoredCommand>>>;
        for body in [
            br#"{"body":["","","","",""],"status":"Ok"}"#.as_slice(),
            br#"{"body":{"large":{"nested":["","",""]}},"status":"Ok"}"#.as_slice(),
        ] {
            let wire = [
                br#"{"version":5,"remaining_deadline_ms":1,"body":"#.as_slice(),
                body,
                br#"}"#.as_slice(),
            ]
            .concat();
            let limits = RecorderDecodeLimits::for_wire_bytes(wire.len());
            let budget = Rc::new(RefCell::new(DecodeBudget::new(limits)));
            let mut deserializer = serde_json::Deserializer::from_slice(&wire);
            let result = ResponseRoot::deserialize(BoundedDeserializer::new(
                &mut deserializer,
                budget.clone(),
            ));
            assert!(result.is_err());
            assert_eq!(budget.borrow().collection_items, 0);
            assert_eq!(budget.borrow().heap_bytes, 0);
        }
    }

    #[test]
    fn recorder_v2_json_status_first_body_uses_the_bounded_payload_path() {
        type ResponseRoot =
            crate::RecorderWire<crate::RecorderV2Result<Option<rhiza_core::StoredCommand>>>;
        // This is schema-valid StoredCommand JSON. The first payload byte is
        // admitted; the second must trip DynamicSeq's shared item budget. If
        // RecorderV2Result decoded the body outside BoundedDeserializer, this
        // assertion would instead succeed after allocating the entire Vec.
        let wire = br#"{"version":5,"remaining_deadline_ms":1,"body":{"status":"Ok","body":{"entry_type":"Command","payload":[1,2,3]}}}"#;
        let limits = RecorderDecodeLimits {
            max_wire_bytes: wire.len(),
            max_decode_heap_bytes: 64,
            max_collection_items: 1,
        };
        let budget = Rc::new(RefCell::new(DecodeBudget::new(limits)));
        let mut deserializer = serde_json::Deserializer::from_slice(wire);
        let result =
            ResponseRoot::deserialize(BoundedDeserializer::new(&mut deserializer, budget.clone()));
        assert!(result.is_err());
        assert_eq!(
            budget.borrow().failure(),
            Some("recorder collection item budget exceeded")
        );
        assert_eq!(budget.borrow().collection_items, 1);
        assert_eq!(budget.borrow().heap_bytes, 8);
    }

    #[test]
    fn recorder_v2_json_rejects_missing_duplicate_and_extra_fields() {
        for body in [
            br#"{"status":"Ok"}"#.as_slice(),
            br#"{"status":"Ok","body":7,"body":8}"#.as_slice(),
            br#"{"status":"Ok","body":7,"extra":0}"#.as_slice(),
        ] {
            assert!(serde_json::from_slice::<crate::RecorderV2Result<u64>>(body).is_err());
        }
    }

    #[test]
    fn decision_proof_summary_collection_is_rejected_before_large_vec_growth() {
        let summary = RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        let proof = DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            proposal: Proposal::new(
                ProposalPriority::from_u64(1),
                "node",
                1,
                rhiza_quepaxa::AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            summaries: vec![summary; 96],
        };
        let bytes = postcard::to_allocvec(&proof).unwrap();
        let per_summary = std::mem::size_of::<RecorderSummary>() * 3;
        let limits = RecorderDecodeLimits {
            max_wire_bytes: bytes.len(),
            // All prefix fields are fixed-layout. This admits a small prefix
            // of summaries, then rejects a later summary seed before a large
            // Vec can be constructed.
            max_decode_heap_bytes: per_summary * 8 + 1024,
            max_collection_items: bytes.len(),
        };
        let budget = Rc::new(RefCell::new(DecodeBudget::new(limits)));
        let mut deserializer = postcard::Deserializer::from_bytes(&bytes);
        let result =
            DecisionProof::deserialize(BoundedDeserializer::new(&mut deserializer, budget.clone()));
        assert!(result.is_err());
        assert!(budget.borrow().collection_items > 0);
        assert!(budget.borrow().collection_items < 96);
        assert!(budget.borrow().heap_bytes < limits.max_decode_heap_bytes);
    }

    #[test]
    fn json_many_empty_values_is_rejected_by_item_budget() {
        let json = b"[\"\",\"\",\"\",\"\",\"\",\"\"]";
        let limits = RecorderDecodeLimits {
            max_wire_bytes: json.len(),
            max_decode_heap_bytes: usize::MAX,
            max_collection_items: 5,
        };
        assert!(matches!(
            decode_json_for_test::<Vec<String>>(json, limits),
            Err(RecorderDecodeError::Decode(message)) if message.contains("item budget exceeded")
        ));

        let objects = b"[{},{},{},{},{},{}]";
        let limits = RecorderDecodeLimits {
            max_wire_bytes: objects.len(),
            max_decode_heap_bytes: usize::MAX,
            max_collection_items: 5,
        };
        assert!(matches!(
            decode_json_for_test::<Vec<Empty>>(objects, limits),
            Err(RecorderDecodeError::Decode(message)) if message.contains("item budget exceeded")
        ));
    }

    #[test]
    fn maxish_byte_vector_succeeds_with_default_budget() {
        let value = vec![7_u8; 64 * 1024];
        let bytes = postcard::to_allocvec(&value).unwrap();
        assert_eq!(
            decode_postcard_for_test::<Vec<u8>>(
                &bytes,
                RecorderDecodeLimits::for_wire_bytes(bytes.len()),
            )
            .unwrap(),
            value
        );
    }

    #[test]
    fn dynamic_sequence_hides_size_hint_before_each_item() {
        let bytes = postcard::to_allocvec(&vec![1_u8, 2, 3]).unwrap();
        assert!(decode_postcard_for_test::<SizeHintChecked>(
            &bytes,
            RecorderDecodeLimits::for_wire_bytes(bytes.len()),
        )
        .is_ok());
    }

    #[test]
    fn boxed_record_summary_reserves_the_audited_boxed_heap_before_decoding_fields() {
        let value = Some(Box::new(RecordSummary {
            recorder_id: "r".into(),
            slot: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            step: 1,
            first_current: None,
            aggregate_prior: None,
            decided: None,
        }));
        let bytes = postcard::to_allocvec(&value).unwrap();
        let limits = RecorderDecodeLimits {
            max_wire_bytes: bytes.len(),
            max_decode_heap_bytes: std::mem::size_of::<RecordSummary>() - 1,
            max_collection_items: bytes.len(),
        };
        let decoded = decode_postcard_for_test::<Option<Box<RecordSummary>>>(&bytes, limits);
        assert!(
            matches!(
                decoded,
                Err(RecorderDecodeError::Decode(ref message)) if message.contains("heap budget exceeded")
            ),
            "{decoded:?}"
        );
    }

    #[test]
    fn depth_limit_and_error_unwind_leave_the_budget_reusable() {
        let budget = Rc::new(RefCell::new(DecodeBudget::new(
            RecorderDecodeLimits::for_wire_bytes(1024),
        )));
        for _ in 0..128 {
            budget.borrow_mut().enter().unwrap();
        }
        assert!(budget.borrow_mut().enter().is_err());
        for _ in 0..128 {
            budget.borrow_mut().leave();
        }
        assert_eq!(budget.borrow().nesting_depth, 0);

        let result: Result<(), serde::de::value::Error> = super::with_depth(&budget, || {
            Err(serde::de::Error::custom("synthetic visitor failure"))
        });
        assert!(result.is_err());
        assert_eq!(budget.borrow().nesting_depth, 0);
        budget.borrow_mut().enter().unwrap();
        budget.borrow_mut().leave();
    }

    #[test]
    fn json_end_accepts_whitespace_rejects_nonspace_and_handles_escaped_scratch() {
        let whitespace = b"[1]\n \t";
        assert_eq!(
            decode_json_for_test::<Vec<u8>>(whitespace, growth_limits(whitespace)).unwrap(),
            vec![1]
        );
        let nonspace = b"[1] x";
        assert!(matches!(
            decode_json_for_test::<Vec<u8>>(nonspace, growth_limits(nonspace)),
            Err(RecorderDecodeError::Decode(_))
        ));
        let escaped = br#""\u0061\u0062""#;
        assert_eq!(
            decode_json_for_test::<String>(
                escaped,
                RecorderDecodeLimits::for_wire_bytes(escaped.len())
            )
            .unwrap(),
            "ab"
        );
    }

    #[test]
    fn huge_postcard_declared_lengths_fail_without_allocation() {
        // postcard string/bytes lengths are unsigned varints.  These tiny
        // messages declare more data than the slice contains; postcard's Slice
        // flavor rejects them before a visitor can allocate an owned value.
        let impossible = [0xff, 0xff, 0xff, 0xff, 0x0f];
        let limits = RecorderDecodeLimits::for_wire_bytes(impossible.len());
        assert!(matches!(
            decode_postcard_for_test::<String>(&impossible, limits),
            Err(RecorderDecodeError::Decode(_))
        ));
        assert!(matches!(
            decode_postcard_for_test::<Vec<u8>>(&impossible, limits),
            Err(RecorderDecodeError::Decode(_))
        ));
        let limits = RecorderDecodeLimits {
            max_wire_bytes: impossible.len(),
            max_decode_heap_bytes: usize::MAX,
            max_collection_items: 3,
        };
        assert!(matches!(
            decode_postcard_for_test::<Vec<()>>(&impossible, limits),
            Err(RecorderDecodeError::Decode(message)) if message.contains("item budget exceeded")
        ));
    }

    #[test]
    fn dynamic_map_fails_closed_and_fixed_enum_trailing_bytes_are_rejected() {
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();
        map.insert(
            "key".to_owned(),
            Some(Choice::Struct { values: vec![1, 2] }),
        );
        let bytes = postcard::to_allocvec(&map).unwrap();
        let decoded = decode_postcard_for_test::<BTreeMap<String, Option<Choice>>>(
            &bytes,
            growth_limits(&bytes),
        );
        assert!(matches!(decoded, Err(RecorderDecodeError::Decode(_))));

        let fixed = postcard::to_allocvec(&Some(Choice::Struct { values: vec![1, 2] })).unwrap();
        let mut trailing = fixed.clone();
        trailing.push(0);
        assert!(matches!(
            decode_postcard_for_test::<Option<Choice>>(&trailing, growth_limits(&trailing)),
            Err(RecorderDecodeError::TrailingBytes)
        ));
    }

    #[test]
    fn limit_construction_and_wire_cap_handle_overflow() {
        assert_eq!(
            RecorderDecodeLimits::for_wire_bytes(13),
            RecorderDecodeLimits {
                max_wire_bytes: 13,
                max_decode_heap_bytes: 52,
                max_collection_items: 13,
            }
        );
        let limits = RecorderDecodeLimits::for_wire_bytes(usize::MAX);
        assert_eq!(limits.max_decode_heap_bytes, usize::MAX);
        assert_eq!(limits.max_collection_items, usize::MAX);
        assert!(matches!(
            decode_json_for_test::<Vec<()>>(
                b"[]",
                RecorderDecodeLimits {
                    max_wire_bytes: 1,
                    max_decode_heap_bytes: 1,
                    max_collection_items: 1,
                }
            ),
            Err(RecorderDecodeError::WireTooLarge { .. })
        ));
    }
}
