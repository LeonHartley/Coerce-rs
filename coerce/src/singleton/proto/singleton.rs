// This file is generated by rust-protobuf 3.4.0. Do not edit
// .proto file is parsed by protoc 3.13.0
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt::skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_results)]
#![allow(unused_mut)]

//! Generated file from `singleton.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_3_4_0;

// @@protoc_insertion_point(message:coerce.singleton.GetStatus)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct GetStatus {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.GetStatus.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.GetStatus.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a GetStatus {
    fn default() -> &'a GetStatus {
        <GetStatus as ::protobuf::Message>::default_instance()
    }
}

impl GetStatus {
    pub fn new() -> GetStatus {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &GetStatus| { &m.source_node_id },
            |m: &mut GetStatus| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<GetStatus>(
            "GetStatus",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for GetStatus {
    const NAME: &'static str = "GetStatus";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> GetStatus {
        GetStatus::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static GetStatus {
        static instance: GetStatus = GetStatus {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for GetStatus {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("GetStatus").unwrap()).clone()
    }
}

impl ::std::fmt::Display for GetStatus {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetStatus {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.ManagerStatus)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct ManagerStatus {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.ManagerStatus.singleton_state)
    pub singleton_state: ::protobuf::EnumOrUnknown<SingletonState>,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.ManagerStatus.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a ManagerStatus {
    fn default() -> &'a ManagerStatus {
        <ManagerStatus as ::protobuf::Message>::default_instance()
    }
}

impl ManagerStatus {
    pub fn new() -> ManagerStatus {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "singleton_state",
            |m: &ManagerStatus| { &m.singleton_state },
            |m: &mut ManagerStatus| { &mut m.singleton_state },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<ManagerStatus>(
            "ManagerStatus",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for ManagerStatus {
    const NAME: &'static str = "ManagerStatus";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.singleton_state = is.read_enum_or_unknown()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.singleton_state != ::protobuf::EnumOrUnknown::new(SingletonState::JOINING) {
            my_size += ::protobuf::rt::int32_size(1, self.singleton_state.value());
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.singleton_state != ::protobuf::EnumOrUnknown::new(SingletonState::JOINING) {
            os.write_enum(1, ::protobuf::EnumOrUnknown::value(&self.singleton_state))?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> ManagerStatus {
        ManagerStatus::new()
    }

    fn clear(&mut self) {
        self.singleton_state = ::protobuf::EnumOrUnknown::new(SingletonState::JOINING);
        self.special_fields.clear();
    }

    fn default_instance() -> &'static ManagerStatus {
        static instance: ManagerStatus = ManagerStatus {
            singleton_state: ::protobuf::EnumOrUnknown::from_i32(0),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for ManagerStatus {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("ManagerStatus").unwrap()).clone()
    }
}

impl ::std::fmt::Display for ManagerStatus {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ManagerStatus {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.RequestLease)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct RequestLease {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.RequestLease.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.RequestLease.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a RequestLease {
    fn default() -> &'a RequestLease {
        <RequestLease as ::protobuf::Message>::default_instance()
    }
}

impl RequestLease {
    pub fn new() -> RequestLease {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &RequestLease| { &m.source_node_id },
            |m: &mut RequestLease| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<RequestLease>(
            "RequestLease",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for RequestLease {
    const NAME: &'static str = "RequestLease";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> RequestLease {
        RequestLease::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static RequestLease {
        static instance: RequestLease = RequestLease {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for RequestLease {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("RequestLease").unwrap()).clone()
    }
}

impl ::std::fmt::Display for RequestLease {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RequestLease {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.LeaseAck)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct LeaseAck {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.LeaseAck.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.LeaseAck.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a LeaseAck {
    fn default() -> &'a LeaseAck {
        <LeaseAck as ::protobuf::Message>::default_instance()
    }
}

impl LeaseAck {
    pub fn new() -> LeaseAck {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &LeaseAck| { &m.source_node_id },
            |m: &mut LeaseAck| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<LeaseAck>(
            "LeaseAck",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for LeaseAck {
    const NAME: &'static str = "LeaseAck";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> LeaseAck {
        LeaseAck::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static LeaseAck {
        static instance: LeaseAck = LeaseAck {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for LeaseAck {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("LeaseAck").unwrap()).clone()
    }
}

impl ::std::fmt::Display for LeaseAck {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for LeaseAck {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.LeaseNack)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct LeaseNack {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.LeaseNack.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.LeaseNack.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a LeaseNack {
    fn default() -> &'a LeaseNack {
        <LeaseNack as ::protobuf::Message>::default_instance()
    }
}

impl LeaseNack {
    pub fn new() -> LeaseNack {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &LeaseNack| { &m.source_node_id },
            |m: &mut LeaseNack| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<LeaseNack>(
            "LeaseNack",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for LeaseNack {
    const NAME: &'static str = "LeaseNack";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> LeaseNack {
        LeaseNack::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static LeaseNack {
        static instance: LeaseNack = LeaseNack {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for LeaseNack {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("LeaseNack").unwrap()).clone()
    }
}

impl ::std::fmt::Display for LeaseNack {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for LeaseNack {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.SingletonStarted)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct SingletonStarted {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.SingletonStarted.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.SingletonStarted.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a SingletonStarted {
    fn default() -> &'a SingletonStarted {
        <SingletonStarted as ::protobuf::Message>::default_instance()
    }
}

impl SingletonStarted {
    pub fn new() -> SingletonStarted {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &SingletonStarted| { &m.source_node_id },
            |m: &mut SingletonStarted| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<SingletonStarted>(
            "SingletonStarted",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for SingletonStarted {
    const NAME: &'static str = "SingletonStarted";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> SingletonStarted {
        SingletonStarted::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static SingletonStarted {
        static instance: SingletonStarted = SingletonStarted {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for SingletonStarted {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("SingletonStarted").unwrap()).clone()
    }
}

impl ::std::fmt::Display for SingletonStarted {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SingletonStarted {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.SingletonStopping)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct SingletonStopping {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.SingletonStopping.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.SingletonStopping.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a SingletonStopping {
    fn default() -> &'a SingletonStopping {
        <SingletonStopping as ::protobuf::Message>::default_instance()
    }
}

impl SingletonStopping {
    pub fn new() -> SingletonStopping {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &SingletonStopping| { &m.source_node_id },
            |m: &mut SingletonStopping| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<SingletonStopping>(
            "SingletonStopping",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for SingletonStopping {
    const NAME: &'static str = "SingletonStopping";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> SingletonStopping {
        SingletonStopping::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static SingletonStopping {
        static instance: SingletonStopping = SingletonStopping {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for SingletonStopping {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("SingletonStopping").unwrap()).clone()
    }
}

impl ::std::fmt::Display for SingletonStopping {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SingletonStopping {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:coerce.singleton.SingletonStopped)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct SingletonStopped {
    // message fields
    // @@protoc_insertion_point(field:coerce.singleton.SingletonStopped.source_node_id)
    pub source_node_id: u64,
    // special fields
    // @@protoc_insertion_point(special_field:coerce.singleton.SingletonStopped.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a SingletonStopped {
    fn default() -> &'a SingletonStopped {
        <SingletonStopped as ::protobuf::Message>::default_instance()
    }
}

impl SingletonStopped {
    pub fn new() -> SingletonStopped {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(1);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "source_node_id",
            |m: &SingletonStopped| { &m.source_node_id },
            |m: &mut SingletonStopped| { &mut m.source_node_id },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<SingletonStopped>(
            "SingletonStopped",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for SingletonStopped {
    const NAME: &'static str = "SingletonStopped";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.source_node_id = is.read_uint64()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.source_node_id != 0 {
            my_size += ::protobuf::rt::uint64_size(1, self.source_node_id);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.source_node_id != 0 {
            os.write_uint64(1, self.source_node_id)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> SingletonStopped {
        SingletonStopped::new()
    }

    fn clear(&mut self) {
        self.source_node_id = 0;
        self.special_fields.clear();
    }

    fn default_instance() -> &'static SingletonStopped {
        static instance: SingletonStopped = SingletonStopped {
            source_node_id: 0,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for SingletonStopped {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("SingletonStopped").unwrap()).clone()
    }
}

impl ::std::fmt::Display for SingletonStopped {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SingletonStopped {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

#[derive(Clone,Copy,PartialEq,Eq,Debug,Hash)]
// @@protoc_insertion_point(enum:coerce.singleton.SingletonState)
pub enum SingletonState {
    // @@protoc_insertion_point(enum_value:coerce.singleton.SingletonState.JOINING)
    JOINING = 0,
    // @@protoc_insertion_point(enum_value:coerce.singleton.SingletonState.IDLE)
    IDLE = 1,
    // @@protoc_insertion_point(enum_value:coerce.singleton.SingletonState.STARTING)
    STARTING = 2,
    // @@protoc_insertion_point(enum_value:coerce.singleton.SingletonState.RUNNING)
    RUNNING = 3,
    // @@protoc_insertion_point(enum_value:coerce.singleton.SingletonState.STOPPING)
    STOPPING = 4,
}

impl ::protobuf::Enum for SingletonState {
    const NAME: &'static str = "SingletonState";

    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<SingletonState> {
        match value {
            0 => ::std::option::Option::Some(SingletonState::JOINING),
            1 => ::std::option::Option::Some(SingletonState::IDLE),
            2 => ::std::option::Option::Some(SingletonState::STARTING),
            3 => ::std::option::Option::Some(SingletonState::RUNNING),
            4 => ::std::option::Option::Some(SingletonState::STOPPING),
            _ => ::std::option::Option::None
        }
    }

    fn from_str(str: &str) -> ::std::option::Option<SingletonState> {
        match str {
            "JOINING" => ::std::option::Option::Some(SingletonState::JOINING),
            "IDLE" => ::std::option::Option::Some(SingletonState::IDLE),
            "STARTING" => ::std::option::Option::Some(SingletonState::STARTING),
            "RUNNING" => ::std::option::Option::Some(SingletonState::RUNNING),
            "STOPPING" => ::std::option::Option::Some(SingletonState::STOPPING),
            _ => ::std::option::Option::None
        }
    }

    const VALUES: &'static [SingletonState] = &[
        SingletonState::JOINING,
        SingletonState::IDLE,
        SingletonState::STARTING,
        SingletonState::RUNNING,
        SingletonState::STOPPING,
    ];
}

impl ::protobuf::EnumFull for SingletonState {
    fn enum_descriptor() -> ::protobuf::reflect::EnumDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().enum_by_package_relative_name("SingletonState").unwrap()).clone()
    }

    fn descriptor(&self) -> ::protobuf::reflect::EnumValueDescriptor {
        let index = *self as usize;
        Self::enum_descriptor().value_by_index(index)
    }
}

impl ::std::default::Default for SingletonState {
    fn default() -> Self {
        SingletonState::JOINING
    }
}

impl SingletonState {
    fn generated_enum_descriptor_data() -> ::protobuf::reflect::GeneratedEnumDescriptorData {
        ::protobuf::reflect::GeneratedEnumDescriptorData::new::<SingletonState>("SingletonState")
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0fsingleton.proto\x12\x10coerce.singleton\"1\n\tGetStatus\x12$\n\x0e\
    source_node_id\x18\x01\x20\x01(\x04R\x0csourceNodeId\"Z\n\rManagerStatus\
    \x12I\n\x0fsingleton_state\x18\x01\x20\x01(\x0e2\x20.coerce.singleton.Si\
    ngletonStateR\x0esingletonState\"4\n\x0cRequestLease\x12$\n\x0esource_no\
    de_id\x18\x01\x20\x01(\x04R\x0csourceNodeId\"0\n\x08LeaseAck\x12$\n\x0es\
    ource_node_id\x18\x01\x20\x01(\x04R\x0csourceNodeId\"1\n\tLeaseNack\x12$\
    \n\x0esource_node_id\x18\x01\x20\x01(\x04R\x0csourceNodeId\"8\n\x10Singl\
    etonStarted\x12$\n\x0esource_node_id\x18\x01\x20\x01(\x04R\x0csourceNode\
    Id\"9\n\x11SingletonStopping\x12$\n\x0esource_node_id\x18\x01\x20\x01(\
    \x04R\x0csourceNodeId\"8\n\x10SingletonStopped\x12$\n\x0esource_node_id\
    \x18\x01\x20\x01(\x04R\x0csourceNodeId*P\n\x0eSingletonState\x12\x0b\n\
    \x07JOINING\x10\0\x12\x08\n\x04IDLE\x10\x01\x12\x0c\n\x08STARTING\x10\
    \x02\x12\x0b\n\x07RUNNING\x10\x03\x12\x0c\n\x08STOPPING\x10\x04b\x06prot\
    o3\
";

/// `FileDescriptorProto` object which was a source for this generated file
fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    static file_descriptor_proto_lazy: ::protobuf::rt::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::Lazy::new();
    file_descriptor_proto_lazy.get(|| {
        ::protobuf::Message::parse_from_bytes(file_descriptor_proto_data).unwrap()
    })
}

/// `FileDescriptor` object which allows dynamic access to files
pub fn file_descriptor() -> &'static ::protobuf::reflect::FileDescriptor {
    static generated_file_descriptor_lazy: ::protobuf::rt::Lazy<::protobuf::reflect::GeneratedFileDescriptor> = ::protobuf::rt::Lazy::new();
    static file_descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::FileDescriptor> = ::protobuf::rt::Lazy::new();
    file_descriptor.get(|| {
        let generated_file_descriptor = generated_file_descriptor_lazy.get(|| {
            let mut deps = ::std::vec::Vec::with_capacity(0);
            let mut messages = ::std::vec::Vec::with_capacity(8);
            messages.push(GetStatus::generated_message_descriptor_data());
            messages.push(ManagerStatus::generated_message_descriptor_data());
            messages.push(RequestLease::generated_message_descriptor_data());
            messages.push(LeaseAck::generated_message_descriptor_data());
            messages.push(LeaseNack::generated_message_descriptor_data());
            messages.push(SingletonStarted::generated_message_descriptor_data());
            messages.push(SingletonStopping::generated_message_descriptor_data());
            messages.push(SingletonStopped::generated_message_descriptor_data());
            let mut enums = ::std::vec::Vec::with_capacity(1);
            enums.push(SingletonState::generated_enum_descriptor_data());
            ::protobuf::reflect::GeneratedFileDescriptor::new_generated(
                file_descriptor_proto(),
                deps,
                messages,
                enums,
            )
        });
        ::protobuf::reflect::FileDescriptor::new_generated_2(generated_file_descriptor)
    })
}
