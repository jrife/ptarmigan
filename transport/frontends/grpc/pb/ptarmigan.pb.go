// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: ptarmigan.proto

package pb

import (
	context "context"
	fmt "fmt"
	raftpb "github.com/coreos/etcd/raft/raftpb"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	ptarmiganpb "github.com/jrife/ptarmigan/transport/ptarmiganpb"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type RaftMessages struct {
	Messages []raftpb.Message `protobuf:"bytes,1,rep,name=messages,proto3" json:"messages"`
}

func (m *RaftMessages) Reset()         { *m = RaftMessages{} }
func (m *RaftMessages) String() string { return proto.CompactTextString(m) }
func (*RaftMessages) ProtoMessage()    {}
func (*RaftMessages) Descriptor() ([]byte, []int) {
	return fileDescriptor_92c36b33a96ae4b7, []int{0}
}
func (m *RaftMessages) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RaftMessages) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RaftMessages.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RaftMessages) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RaftMessages.Merge(m, src)
}
func (m *RaftMessages) XXX_Size() int {
	return m.Size()
}
func (m *RaftMessages) XXX_DiscardUnknown() {
	xxx_messageInfo_RaftMessages.DiscardUnknown(m)
}

var xxx_messageInfo_RaftMessages proto.InternalMessageInfo

func init() {
	proto.RegisterType((*RaftMessages)(nil), "pb.RaftMessages")
}

func init() { proto.RegisterFile("ptarmigan.proto", fileDescriptor_92c36b33a96ae4b7) }

var fileDescriptor_92c36b33a96ae4b7 = []byte{
	// 265 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2f, 0x28, 0x49, 0x2c,
	0xca, 0xcd, 0x4c, 0x4f, 0xcc, 0xd3, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2a, 0x48, 0x92,
	0xd2, 0x4d, 0xcf, 0x2c, 0xc9, 0x28, 0x4d, 0xd2, 0x4b, 0xce, 0xcf, 0xd5, 0x4f, 0xce, 0x2f, 0x4a,
	0xcd, 0x2f, 0xd6, 0x4f, 0x2d, 0x49, 0x4e, 0xd1, 0x2f, 0x4a, 0x4c, 0x2b, 0x01, 0x13, 0x05, 0x49,
	0x60, 0x0a, 0xa2, 0x45, 0x4a, 0xb5, 0xa4, 0x28, 0x31, 0xaf, 0xb8, 0x20, 0xbf, 0xa8, 0x44, 0x1f,
	0x6e, 0x5a, 0x41, 0x92, 0x3e, 0x9a, 0xc9, 0x52, 0x22, 0xe9, 0xf9, 0xe9, 0xf9, 0x60, 0xa6, 0x3e,
	0x88, 0x05, 0x11, 0x55, 0x72, 0xe4, 0xe2, 0x09, 0x4a, 0x4c, 0x2b, 0xf1, 0x4d, 0x2d, 0x2e, 0x4e,
	0x4c, 0x4f, 0x2d, 0x16, 0x32, 0xe4, 0xe2, 0xc8, 0x85, 0xb2, 0x25, 0x18, 0x15, 0x98, 0x35, 0xb8,
	0x8d, 0xf8, 0xf5, 0x20, 0x56, 0xea, 0x41, 0xd5, 0x38, 0xb1, 0x9c, 0xb8, 0x27, 0xcf, 0x10, 0x04,
	0x57, 0x66, 0x54, 0xce, 0xc5, 0x02, 0x32, 0x42, 0xc8, 0x92, 0x8b, 0xd7, 0xb1, 0xa0, 0x20, 0xa7,
	0x32, 0x38, 0x2f, 0xb1, 0xa0, 0x38, 0x23, 0xbf, 0x44, 0x48, 0x48, 0x0f, 0xc9, 0x3d, 0x7a, 0xce,
	0x19, 0xa5, 0x79, 0xd9, 0x52, 0xa8, 0x62, 0xae, 0xb9, 0x05, 0x25, 0x95, 0x1a, 0x8c, 0x42, 0x26,
	0x5c, 0x3c, 0xc1, 0xa9, 0x79, 0x29, 0x70, 0x57, 0x08, 0xe8, 0x15, 0x24, 0xe9, 0x21, 0xbb, 0x0b,
	0x9b, 0x3e, 0x27, 0x85, 0x13, 0x0f, 0xe5, 0x18, 0x2e, 0x3c, 0x94, 0x63, 0x38, 0xf1, 0x48, 0x8e,
	0xf1, 0xc2, 0x23, 0x39, 0xc6, 0x07, 0x8f, 0xe4, 0x18, 0x27, 0x3c, 0x96, 0x63, 0xb8, 0xf0, 0x58,
	0x8e, 0xe1, 0xc6, 0x63, 0x39, 0x86, 0x24, 0x36, 0xb0, 0x27, 0x8d, 0x01, 0x01, 0x00, 0x00, 0xff,
	0xff, 0x6b, 0xce, 0x1d, 0x74, 0x67, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// RaftClient is the client API for Raft service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type RaftClient interface {
	ApplySnapshot(ctx context.Context, opts ...grpc.CallOption) (Raft_ApplySnapshotClient, error)
	SendMessages(ctx context.Context, in *RaftMessages, opts ...grpc.CallOption) (*ptarmiganpb.Empty, error)
}

type raftClient struct {
	cc *grpc.ClientConn
}

func NewRaftClient(cc *grpc.ClientConn) RaftClient {
	return &raftClient{cc}
}

func (c *raftClient) ApplySnapshot(ctx context.Context, opts ...grpc.CallOption) (Raft_ApplySnapshotClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Raft_serviceDesc.Streams[0], "/pb.Raft/ApplySnapshot", opts...)
	if err != nil {
		return nil, err
	}
	x := &raftApplySnapshotClient{stream}
	return x, nil
}

type Raft_ApplySnapshotClient interface {
	Send(*ptarmiganpb.Chunk) error
	CloseAndRecv() (*ptarmiganpb.Empty, error)
	grpc.ClientStream
}

type raftApplySnapshotClient struct {
	grpc.ClientStream
}

func (x *raftApplySnapshotClient) Send(m *ptarmiganpb.Chunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *raftApplySnapshotClient) CloseAndRecv() (*ptarmiganpb.Empty, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(ptarmiganpb.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *raftClient) SendMessages(ctx context.Context, in *RaftMessages, opts ...grpc.CallOption) (*ptarmiganpb.Empty, error) {
	out := new(ptarmiganpb.Empty)
	err := c.cc.Invoke(ctx, "/pb.Raft/SendMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServer is the server API for Raft service.
type RaftServer interface {
	ApplySnapshot(Raft_ApplySnapshotServer) error
	SendMessages(context.Context, *RaftMessages) (*ptarmiganpb.Empty, error)
}

// UnimplementedRaftServer can be embedded to have forward compatible implementations.
type UnimplementedRaftServer struct {
}

func (*UnimplementedRaftServer) ApplySnapshot(srv Raft_ApplySnapshotServer) error {
	return status.Errorf(codes.Unimplemented, "method ApplySnapshot not implemented")
}
func (*UnimplementedRaftServer) SendMessages(ctx context.Context, req *RaftMessages) (*ptarmiganpb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendMessages not implemented")
}

func RegisterRaftServer(s *grpc.Server, srv RaftServer) {
	s.RegisterService(&_Raft_serviceDesc, srv)
}

func _Raft_ApplySnapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RaftServer).ApplySnapshot(&raftApplySnapshotServer{stream})
}

type Raft_ApplySnapshotServer interface {
	SendAndClose(*ptarmiganpb.Empty) error
	Recv() (*ptarmiganpb.Chunk, error)
	grpc.ServerStream
}

type raftApplySnapshotServer struct {
	grpc.ServerStream
}

func (x *raftApplySnapshotServer) SendAndClose(m *ptarmiganpb.Empty) error {
	return x.ServerStream.SendMsg(m)
}

func (x *raftApplySnapshotServer) Recv() (*ptarmiganpb.Chunk, error) {
	m := new(ptarmiganpb.Chunk)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Raft_SendMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RaftMessages)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).SendMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.Raft/SendMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).SendMessages(ctx, req.(*RaftMessages))
	}
	return interceptor(ctx, in, info, handler)
}

var _Raft_serviceDesc = grpc.ServiceDesc{
	ServiceName: "pb.Raft",
	HandlerType: (*RaftServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SendMessages",
			Handler:    _Raft_SendMessages_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ApplySnapshot",
			Handler:       _Raft_ApplySnapshot_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "ptarmigan.proto",
}

func (m *RaftMessages) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RaftMessages) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RaftMessages) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Messages) > 0 {
		for iNdEx := len(m.Messages) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Messages[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintPtarmigan(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func encodeVarintPtarmigan(dAtA []byte, offset int, v uint64) int {
	offset -= sovPtarmigan(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *RaftMessages) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Messages) > 0 {
		for _, e := range m.Messages {
			l = e.Size()
			n += 1 + l + sovPtarmigan(uint64(l))
		}
	}
	return n
}

func sovPtarmigan(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozPtarmigan(x uint64) (n int) {
	return sovPtarmigan(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RaftMessages) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPtarmigan
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RaftMessages: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RaftMessages: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Messages", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPtarmigan
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPtarmigan
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthPtarmigan
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Messages = append(m.Messages, raftpb.Message{})
			if err := m.Messages[len(m.Messages)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPtarmigan(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPtarmigan
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPtarmigan
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipPtarmigan(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowPtarmigan
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPtarmigan
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPtarmigan
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthPtarmigan
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupPtarmigan
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthPtarmigan
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthPtarmigan        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowPtarmigan          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupPtarmigan = fmt.Errorf("proto: unexpected end of group")
)
