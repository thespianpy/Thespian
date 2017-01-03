"""This source file provides functionality for a python-only
implementation of RSA public key verification of a signed message.
This is not necessarily a fast implementation, but by being
python-only it allows for validation of a signed message with a public
key without external dependencies.

This functionality is used by the Thespian Director for its Source
Authority and is available for Director alternatives, but is not
required for Thespian itself.
"""

import sys

if sys.version_info[0] == 2:
    b64inp = lambda s: s
    to_bytelist = lambda s: map(ord, s)
    list_to_str = lambda l: ''.join(map(chr, l))
else:
    b64inp = lambda s: s.encode('ascii')
    to_bytelist = lambda s: list(s)
    import array
    list_to_str = lambda l: array.array('B', l).tostring()

try:
    from functools import reduce # Python3
except Exception:
    pass

def key_factors(key):
    import base64
    key_start = '-----BEGIN PUBLIC KEY-----'
    key_end = '-----END PUBLIC KEY-----'
    key_start_p = key.find(key_start) + len(key_start)
    key_end_p = key.find(key_end, key_start_p)
    keyStr = ''.join(key[key_start_p:key_end_p].strip().split('\n'))
    # Grumble: decodestring doesn't actually take a string under
    # Python3 (but it should)... it wants a bytes object
    keyBinary = to_bytelist(base64.decodestring(b64inp(keyStr)))
    keyInfo, extra = asnDecode(keyBinary)
    return keyInfo[1][0]  # modN, e

# https://msdn.microsoft.com/en-us/library/windows/desktop/bb648645(v=vs.85).aspx

def asnDecode(seq):
    itemId = seq[0]
    itemLen, remSeq = asnDecode_itemLen(seq[1:])
    return { 0x02: asnDecode_Integer,
             0x03: asnDecode_BitString,
             0x05: asnDecode_Null,
             0x06: asnDecode_ObjectID,
             0x30: asnDecode_Seq,
         }[itemId](remSeq, itemLen)

def asnDecode_itemLen(seq):
    seqLen = seq[0]
    seqLenLen = 0
    if seqLen & 0x80:
        seqLenLen = seqLen & 0x7f
        seqLen = seqToInt(seq[1:], seqLenLen)
    return seqLen, seq[1 + seqLenLen:]

def seqToInt(seq, intLen):
    return reduce(lambda a,b: (a << 8) + b, seq[:intLen])

def asnDecode_Integer(seq, seqLen):
    intval = seqToInt(seq, seqLen)
    if seq[0] & 0x80: intval = -intval
    return intval, seq[seqLen:]

def asnDecode_BitString(seq, seqLen):
    leftoverBits = seq[0]
    return asnDecode_Seq(seq[1:], seqLen-1)

def asnDecode_Null(seq, seqLen):
    return None, seq

def asnDecode_Seq(seq, seqLen):
    rem = seq[:seqLen]
    seqData = []
    while rem:
        data, rem = asnDecode(rem)
        seqData.append(data)
    return seqData, seq[seqLen:]

class ObjectID(object):
    def __init__(self, objId):
        self.objId = objId

def asnDecode_ObjectID(seq, idLen):
    return ObjectID(seq[:idLen]), seq[idLen:]

def intToSeq(intval, seqlen):
    seq = []
    while intval:
        seq.insert(0, intval & 0xff)
        intval >>= 8
    return [0] * (seqlen - len(seq)) + seq

rsasig = [0x30, 0x31, 0x30, 0x0d, 6, 9, 0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1, 5, 0, 4, 0x20]

def verify(message, signature, modN, e, hashfunc):
    # https://gist.github.com/FiloSottile/4340076
    chash = hashfunc(list_to_str(message)).digest()
    # S^e = Pad(Hash(M)) (mod N), S is signature, M is message, e and N are params from public key
    intSig = seqToInt(signature, len(signature))
    sigfactor = pow(intSig, e, modN)
    scheck = intToSeq(sigfactor, len(signature))
    try:
        # Signature of an RSA signature
        return scheck[:2] == [0,1] and \
            scheck[scheck.index(0, 2)+1:] == (rsasig + to_bytelist(chash))
            #scheck[scheck.index(0, 2)+1:] == (rsasig + map(ord, chash))
    except (ValueError, IndexError):
        return False

def extract_ascii(inp_data, max_len):
    for cpos in range(1, max_len):
        try:
            inp_data[:cpos].decode('ascii')
        except UnicodeDecodeError:
            return inp_data[:cpos-1].decode('ascii'), inp_data[cpos:]
    return inp_data[:max_len].decode('ascii'), inp_data[max_len:]
