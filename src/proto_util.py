import google.protobuf.message

class ProtoUtils():
    @staticmethod
    def VarSize(uint64):
        if uint64 <= 0x7f: return 1
        if uint64 <= 0x3fff: return 2
        if uint64 <= 0x1fffff: return 3
        if uint64 <= 0xfffffff: return 4
        return 5

    @staticmethod
    def EncodeVarint(value):
        v = []
        bits = value & 0x7f
        value >>= 7
        while value:
            v.append(chr(0x80|bits))
            bits = value & 0x7f
            value >>= 7

        v.append(chr(bits))
        return ''.join(v)

    @staticmethod
    def DecodeVarint(buffer):
        pos = 0
        result = 0
        shift = 0
        mask = (1 << 64) - 1
        while 1:
            b = ord(buffer[pos])
            result |= ((b & 0x7f) << shift)
            pos += 1

            if not (b & 0x80):
                result &= mask
                return result, pos

            shift += 7
            if shift >= 64:
                raise google.protobuf.message.DecodeError('Too many bytes when decoding varint.')
