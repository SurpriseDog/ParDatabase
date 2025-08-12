import hashlib

TRUNCATE = 64   # Hashes are truncated to 64 hex = 256 bits for space savings in database


MINHASH = 16	# Minimum size of hash = 64 bits
assert(TRUNCATE) >= MINHASH


def hash_cmp(aaa, bbb):
	"Compare hashes of unequal length"
	length = min(len(aaa), len(bbb))
	assert length >= MINHASH
	if aaa[:length] != bbb[:length]:
		return False
	return True
	
	
def get_hash(path, chunk=4 * 1024 * 1024, truncate=64):
	"Get sha512 of filename"
	# print('debug hashing', path)
	m = hashlib.sha512()
	with open(path, 'rb') as f:
		while True:
			try:
				data = f.read(chunk)
			except IOError as err:
				print('\nIO Error in', path)
				print(err)
				return 'ioerror'
			if data:
				m.update(data)
			else:
				# return m.hexdigest()[:2] + base64.urlsafe_b64encode(m.digest()[1:]).decode()
				# on disks savings of 10596 vs 11892 = 11% after lzma compression
				# may be useful for in memory savings in future
				return m.hexdigest()[:truncate]
