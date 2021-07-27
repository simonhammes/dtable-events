import base64
import logging
import os
try:
    from Crypto.Cipher import AES
except ImportError:
    AES = None

dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')
logger = logging.getLogger(__name__)
try:
    import seahub.settings as seahub_settings
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

class AESPasswordDecodeError(Exception):
    pass

# the block size for the cipher object; must be 16, 24, or 32 for AES
BLOCK_SIZE = 32

# the character used for padding--with a block cipher such as AES, the value
# you encrypt must be a multiple of BLOCK_SIZE in length.  This character is
# used to ensure that your value is always a multiple of BLOCK_SIZE
PADDING = '{'

# one-liner to sufficiently pad the text to be encrypted
pad = lambda s: s + (16 - len(s) % 16) * PADDING

# one-liners to encrypt/encode and decrypt/decode a string
# encrypt with AES, encode with base64
EncodeAES = lambda c, s: base64.b64encode(c.encrypt(pad(s).encode('utf-8'))).decode('utf-8')
DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).decode('utf-8').rstrip(PADDING)

class AESPasswordHasher:
    algorithm = 'aes'

    def __init__(self, secret=None):
        if not secret:
            secret = seahub_settings.SECRET_KEY[:BLOCK_SIZE]
        self.cipher = AES.new(secret.encode('utf-8'), AES.MODE_ECB)

    def decode(self, encoded):
        algorithm, data = encoded.split('$', 1)
        if algorithm != self.algorithm:
            raise AESPasswordDecodeError
        data = data.encode('utf-8')

        return DecodeAES(self.cipher, data)
