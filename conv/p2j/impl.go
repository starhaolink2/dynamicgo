package p2j

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/cloudwego/dynamicgo/http"
	"github.com/cloudwego/dynamicgo/internal/json"
	"github.com/cloudwego/dynamicgo/internal/rt"
	"github.com/cloudwego/dynamicgo/meta"
	"github.com/cloudwego/dynamicgo/proto"
	"github.com/cloudwego/dynamicgo/proto/binary"
)

const (
	_GUARD_SLICE_FACTOR = 2
)

func wrapError(code meta.ErrCode, msg string, err error) error {
	return meta.NewError(code, msg, err)
}

//go:noinline
func unwrapError(msg string, err error) error {
	if v, ok := err.(meta.Error); ok {
		return wrapError(v.Code, msg, err)
	} else {
		return wrapError(meta.ErrConvert, msg, err)
	}
}

func (self *BinaryConv) do(ctx context.Context, src []byte, desc *proto.TypeDescriptor, out *[]byte, resp http.ResponseSetter) (err error) {
	//NOTICE: output buffer must be larger than src buffer
	rt.GuardSlice(out, len(src)*_GUARD_SLICE_FACTOR)

	var p = binary.BinaryProtocol{
		Buf: src,
	}
	protoLen := len(src)
	// when desc is Singular/Map/List
	if desc.Type() != proto.MESSAGE {
		wtyp := proto.Kind2Wire[protoreflect.Kind(desc.Type())]
		return self.doRecurse(ctx, desc, out, resp, &p, wtyp, protoLen)
	}

	// when desc is Message
	messageDesc := desc.Message()
	return self.unmarshalMessage(ctx, resp, &p, out, messageDesc, protoLen)
}

// Parse ProtoData into JSONData by DescriptorType
func (self *BinaryConv) doRecurse(ctx context.Context, fd *proto.TypeDescriptor, out *[]byte, resp http.ResponseSetter, p *binary.BinaryProtocol, typeId proto.WireType, protoLen int) error {
	switch {
	case (*fd).IsList():
		return self.unmarshalList(ctx, resp, p, typeId, out, fd, protoLen)
	case (*fd).IsMap():
		return self.unmarshalMap(ctx, resp, p, typeId, out, fd, protoLen)
	default:
		return self.unmarshalSingular(ctx, resp, p, out, fd)
	}
}

// parse Singular/MessageType
// field tag is processed outside before doRecurse
// Singular format:	[(L)V]
// Message format: [Length][[Tag][(L)V] [Tag][(L)V]....]
func (self *BinaryConv) unmarshalSingular(ctx context.Context, resp http.ResponseSetter, p *binary.BinaryProtocol, out *[]byte, fd *proto.TypeDescriptor) (err error) {
	switch fd.Type() {
	case proto.BOOL:
		v, e := p.ReadBool()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Boolkind error", e)
		}
		*out = json.EncodeBool(*out, v)
	case proto.ENUM:
		v, e := p.ReadEnum()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Enumkind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.INT32:
		v, e := p.ReadInt32()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Int32kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.SINT32:
		v, e := p.ReadSint32()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Sint32kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.UINT32:
		v, e := p.ReadUint32()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Uint32kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.FIX32:
		v, e := p.ReadFixed32()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Fixed32kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.SFIX32:
		v, e := p.ReadSfixed32()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Sfixed32kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.INT64:
		v, e := p.ReadInt64()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Int64kind error", e)
		}
		if self.opts.Int642String {
			*out = append(*out, '"')
			*out = json.EncodeInt64(*out, int64(v))
			*out = append(*out, '"')
		} else {
			*out = json.EncodeInt64(*out, int64(v))
		}
	case proto.SINT64:
		v, e := p.ReadSint64()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Sint64kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.UINT64:
		v, e := p.ReadUint64()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Uint64kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.FIX64:
		v, e := p.ReadFixed64()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Fixed64kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.SFIX64:
		v, e := p.ReadSfixed64()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Sfixed64kind error", e)
		}
		*out = json.EncodeInt64(*out, int64(v))
	case proto.FLOAT:
		v, e := p.ReadFloat()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Floatkind error", e)
		}
		*out = json.EncodeFloat64(*out, float64(v))
	case proto.DOUBLE:
		v, e := p.ReadDouble()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Doublekind error", e)
		}
		*out = json.EncodeFloat64(*out, float64(v))
	case proto.STRING:
		v, e := p.ReadString(false)
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Stringkind error", e)
		}
		*out = json.EncodeString(*out, v)
	case proto.BYTE:
		v, e := p.ReadBytes()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Byteskind error", e)
		}
		*out = json.EncodeBaniry(*out, v)
	case proto.MESSAGE:
		l, e := p.ReadLength()
		if e != nil {
			return wrapError(meta.ErrRead, "unmarshal Byteskind error", e)
		}
		if err := self.unmarshalMessage(ctx, resp, p, out, (*fd).Message(), l); err != nil {
			return err
		}
	default:
		return wrapError(meta.ErrUnsupportedType, fmt.Sprintf("unknown descriptor type %s", fd.Type()), nil)
	}
	return
}

func (self *BinaryConv) unmarshalMessage(ctx context.Context, resp http.ResponseSetter, p *binary.BinaryProtocol, out *[]byte, messageDesc *proto.MessageDescriptor, protoLen int) (err error) {
	comma := false
	end := p.Read + protoLen
	var seenFields map[proto.FieldNumber]struct{}
	if self.opts.WriteDefaultField {
		seenFields = make(map[proto.FieldNumber]struct{}, messageDesc.FieldsCount())
	}

	*out = json.EncodeObjectBegin(*out)

	for p.Read < end {
		fieldId, typeId, _, e := p.ConsumeTag()
		if e != nil {
			return wrapError(meta.ErrRead, "", e)
		}

		fd := messageDesc.ByNumber(fieldId)
		if fd == nil {
			if self.opts.DisallowUnknownField {
				return wrapError(meta.ErrUnknownField, fmt.Sprintf("unknown field %d", fieldId), nil)
			}
			if e := p.Skip(typeId, false); e != nil {
				return wrapError(meta.ErrRead, "", e)
			}
			continue
		}

		if seenFields != nil {
			seenFields[fieldId] = struct{}{}
		}

		if comma {
			*out = json.EncodeObjectComma(*out)
		} else {
			comma = true
		}

		*out = json.EncodeString(*out, fd.Name())
		*out = json.EncodeObjectColon(*out)
		fieldProtoLen := end - p.Read
		err := self.doRecurse(ctx, fd.Type(), out, resp, p, typeId, fieldProtoLen)
		if err != nil {
			return unwrapError(fmt.Sprintf("converting field %s of MESSAGE %s failed", fd.Name(), fd.Kind()), err)
		}
	}

	if seenFields != nil {
		if err := self.writeMissingDefaultFields(out, messageDesc, seenFields, &comma); err != nil {
			return err
		}
	}

	*out = json.EncodeObjectEnd(*out)
	return nil
}

func (self *BinaryConv) writeMissingDefaultFields(out *[]byte, messageDesc *proto.MessageDescriptor, seenFields map[proto.FieldNumber]struct{}, comma *bool) error {
	for _, field := range messageDesc.Fields() {
		if _, ok := seenFields[field.Number()]; ok {
			continue
		}
		if field.Type().Type() == proto.MESSAGE {
			continue
		}

		if *comma {
			*out = json.EncodeObjectComma(*out)
		} else {
			*comma = true
		}
		*out = json.EncodeString(*out, field.Name())
		*out = json.EncodeObjectColon(*out)
		if field.HasNullableScalarPresence() {
			*out = json.EncodeNull(*out)
			continue
		}
		if err := self.writeDefaultValue(out, field.Type()); err != nil {
			return unwrapError(fmt.Sprintf("writing default field %s failed", field.Name()), err)
		}
	}
	return nil
}

func (self *BinaryConv) writeDefaultValue(out *[]byte, desc *proto.TypeDescriptor) error {
	switch {
	case desc.IsList():
		*out = json.EncodeArrayBegin(*out)
		*out = json.EncodeArrayEnd(*out)
		return nil
	case desc.IsMap():
		*out = json.EncodeObjectBegin(*out)
		*out = json.EncodeObjectEnd(*out)
		return nil
	}

	switch desc.Type() {
	case proto.BOOL:
		*out = json.EncodeBool(*out, false)
	case proto.ENUM:
		*out = json.EncodeInt64(*out, 0)
	case proto.INT32, proto.SINT32, proto.UINT32, proto.FIX32, proto.SFIX32:
		*out = json.EncodeInt64(*out, 0)
	case proto.INT64:
		if self.opts.Int642String {
			*out = append(*out, '"')
			*out = json.EncodeInt64(*out, 0)
			*out = append(*out, '"')
		} else {
			*out = json.EncodeInt64(*out, 0)
		}
	case proto.SINT64, proto.UINT64, proto.FIX64, proto.SFIX64:
		*out = json.EncodeInt64(*out, 0)
	case proto.FLOAT, proto.DOUBLE:
		*out = json.EncodeFloat64(*out, 0)
	case proto.STRING:
		*out = json.EncodeString(*out, "")
	case proto.BYTE:
		*out = json.EncodeBaniry(*out, nil)
	case proto.MESSAGE:
		return nil
	default:
		return wrapError(meta.ErrUnsupportedType, fmt.Sprintf("unknown descriptor type %s", desc.Type()), nil)
	}
	return nil
}

// parse ListType
// Packed List format: [Tag][Length][Value Value Value Value Value]....
// Unpacked List format: [Tag][Length][Value] [Tag][Length][Value]....
func (self *BinaryConv) unmarshalList(ctx context.Context, resp http.ResponseSetter, p *binary.BinaryProtocol, typeId proto.WireType, out *[]byte, fd *proto.TypeDescriptor, protoLen int) (err error) {
	*out = json.EncodeArrayBegin(*out)

	fieldNumber := fd.BaseId()
	// packedList(format)：[Tag] [Length] [Value Value Value Value Value]
	if typeId == proto.BytesType && (*fd).IsPacked() {
		len, err := p.ReadLength()
		if err != nil {
			return wrapError(meta.ErrRead, "unmarshal List Length error", err)
		}
		start := p.Read
		// parse Value repeated
		for p.Read < start+len {
			self.unmarshalSingular(ctx, resp, p, out, fd.Elem())
			if p.Read != start && p.Read != start+len {
				*out = json.EncodeArrayComma(*out)
			}
		}
	} else {
		// unpackedList(format)：[Tag][Length][Value] [Tag][Length][Value]....
		start := p.Read
		itemDesc := fd.Elem()
		for p.Read < start+protoLen {
			itemStartPos := p.Read
			// read Len but do not move the read pointer
			// itemLength is the length of the item value, offset is the byte length of storage length
			itemLength, offset, err := p.ReadLengthWithoutMove()
			if err != nil {
				return wrapError(meta.ErrRead, "unmarshal List item Length error", err)
			}
			itemEndPos := p.Read + offset + itemLength
			itemLen := itemEndPos - itemStartPos
			// unmarshal the nested object inner item
			self.doRecurse(ctx, itemDesc, out, resp, p, typeId, itemLen)
			// check read pointer after unmarshal
			if p.Read >= start+protoLen {
				break
			}

			elementFieldNumber, _, tagLen, err := p.ConsumeTagWithoutMove()
			if err != nil {
				return wrapError(meta.ErrRead, "consume list child Tag error", err)
			}
			// List parse end, pay attention to remove the last ','
			if elementFieldNumber != fieldNumber {
				break
			}
			*out = json.EncodeArrayComma(*out)
			// read the tag of the next list item
			p.Read += tagLen
		}
	}

	*out = json.EncodeArrayEnd(*out)
	return nil
}

// parse MapType
// Map bytes format: [Pairtag][Pairlength][keyTag(L)V][valueTag(L)V] [Pairtag][Pairlength][T(L)V][T(L)V]...
// Pairtag = MapFieldnumber << 3 | wiretype:BytesType
func (self *BinaryConv) unmarshalMap(ctx context.Context, resp http.ResponseSetter, p *binary.BinaryProtocol, typeId proto.WireType, out *[]byte, fd *proto.TypeDescriptor, protoLen int) (err error) {
	start := p.Read
	fieldNumber := (*fd).BaseId()
	_, lengthErr := p.ReadLength()
	if lengthErr != nil {
		return wrapError(meta.ErrRead, "parse Tag length error", err)
	}

	*out = json.EncodeObjectBegin(*out)

	// parse first k-v pair, [KeyTag][KeyLength][KeyValue][ValueTag][ValueLength][ValueValue]
	_, _, _, keyErr := p.ConsumeTag()
	if keyErr != nil {
		return wrapError(meta.ErrRead, "parse MapKey Tag error", err)
	}
	mapKeyDesc := fd.Key()
	isIntKey := (mapKeyDesc.Type() == proto.INT32) || (mapKeyDesc.Type() == proto.INT64) || (mapKeyDesc.Type() == proto.UINT32) || (mapKeyDesc.Type() == proto.UINT64)
	if isIntKey {
		*out = append(*out, '"')
	}
	if self.unmarshalSingular(ctx, resp, p, out, mapKeyDesc) != nil {
		return wrapError(meta.ErrRead, "parse MapKey Value error", err)
	}
	if isIntKey {
		*out = append(*out, '"')
	}
	*out = json.EncodeObjectColon(*out)
	_, _, _, valueErr := p.ConsumeTag()
	if valueErr != nil {
		return wrapError(meta.ErrRead, "parse MapValue Tag error", err)
	}
	mapValueDesc := fd.Elem()
	if self.unmarshalSingular(ctx, resp, p, out, mapValueDesc) != nil {
		return wrapError(meta.ErrRead, "parse MapValue Value error", err)
	}
	// parse the remaining k-v pairs
	for p.Read < start+protoLen {
		pairNumber, _, tagLen, err := p.ConsumeTagWithoutMove()
		if err != nil {
			return wrapError(meta.ErrRead, "consume map child Tag error", err)
		}
		// parse second Tag
		if pairNumber != fieldNumber {
			break
		}
		p.Read += tagLen
		*out = json.EncodeObjectComma(*out)
		// parse second length
		_, lengthErr := p.ReadLength()
		if lengthErr != nil {
			return wrapError(meta.ErrRead, "parse Tag length error", err)
		}
		// parse second [KeyTag][KeyLength][KeyValue][ValueTag][ValueLength][ValueValue]
		_, _, _, keyErr = p.ConsumeTag()
		if keyErr != nil {
			return wrapError(meta.ErrRead, "parse MapKey Tag error", err)
		}
		if isIntKey {
			*out = append(*out, '"')
		}
		if self.unmarshalSingular(ctx, resp, p, out, mapKeyDesc) != nil {
			return wrapError(meta.ErrRead, "parse MapKey Value error", err)
		}
		if isIntKey {
			*out = append(*out, '"')
		}
		*out = json.EncodeObjectColon(*out)
		_, _, _, valueErr = p.ConsumeTag()
		if valueErr != nil {
			return wrapError(meta.ErrRead, "parse MapValue Tag error", err)
		}
		if self.unmarshalSingular(ctx, resp, p, out, mapValueDesc) != nil {
			return wrapError(meta.ErrRead, "parse MapValue Value error", err)
		}
	}

	*out = json.EncodeObjectEnd(*out)
	return nil
}
