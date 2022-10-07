import binascii
import datetime
import os
import pathlib
import sys
import time


class bytesize(int):
	PREFIXES = ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi")
	def __str__(self):
		i = min(len(self.__class__.PREFIXES) - 1, (max(1, self.bit_length()) - 1) // 10)
		p = self.__class__.PREFIXES[i]
		return f"{self / 1024**i:.1f}{p}B"

def checksum_block(file, size=4096):
	crc = 0
	while data := file.read(size): crc = binascii.crc32(data, crc)
	return format(crc, "08X")

def fileinfo(path):
	with open(path, "rb", 0) as f:
		size = os.path.getsize(path)
		checksum = checksum_block(f)
		return (path, size, checksum)	

class DirTree:
	def __init__(self, root):
		self.root = pathlib.Path(root).resolve()
		self.restartpath = None

	def walk(self, restart=None):
		self._skipped = 0
		self._processed = 0
		self._erroneous = 0
		self._processed_bytes = 0
		self._tree = os.walk(self.root)
		if restart:
			skip = True
			self.restartpath = (self.root / restart).resolve(True)
			for path,dirs,files in self._tree:
				path = pathlib.Path(path)
				if path == self.restartpath: skip = False
				for file in files:
					fullpath = (path / file).resolve()
					if fullpath == self.restartpath: skip = False
					if skip: self._skipped += 1
					else:
						try:
							yield self._procfile(fullpath)
						except Exception as exc:
							continue
				if not skip: break
		for path,dirs,files in self._tree:
			path = pathlib.Path(path)
			for file in files:
				fullpath = (path / file).resolve()
				try:
					yield self._procfile(fullpath)
				except Exception as exc:
					continue				

	@property
	def skipped(self):
		return self._skipped

	@property
	def processed(self):
		return self._processed

	@property
	def erroneous(self):
		return self._erroneous

	@property
	def processed_bytes(self):
		return self._processed_bytes

	def _procfile(self, path):
		try:
			info = fileinfo(path)
			path, size = info[:2]
		except Exception as exc:
			self._erroneous += 1 
			print(f"Error processing file {path}: {exc}", file=sys.stderr)
			raise
		else:
			self._processed += 1
			self._processed_bytes += size
			return info

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument("directory", type=str, help="target directory")
	parser.add_argument("-o", "--output", type=str, help="save output to file")
	parser.add_argument("-f", "--flush", type=int, default=2048, help="autosave file every n MB")
	parser.add_argument("-r", "--resume", type=str, help="resume traversing the tree from this file or directory")
	args = parser.parse_args()
	root = args.directory
	if args.output: output = open(args.output, "w", encoding="UTF-8")
	else: output = sys.stdout
	tree = DirTree(root)
	print(f"ROOT: {tree.root}", file=output)
	last_flush = 0
	t_0 = time.perf_counter()
	for path, size, checksum in tree.walk(args.resume):
		print("{},{},{}".format(path.relative_to(tree.root), size, checksum), file=output)
		if tree.processed_bytes - last_flush >= args.flush * 2**20:
			output.flush()
			last_flush = tree.processed_bytes
	total_time = time.perf_counter() - t_0
	speed = tree.processed_bytes / total_time
	print(f"Skipped files: {tree.skipped}\nProcessed files: {tree.processed}\nErrors: {tree.erroneous}\nElapsed time: {datetime.timedelta(seconds=total_time)}\nBytes processed: {tree.processed_bytes}B\nSpeed: {bytesize(speed)}/s")
	if args.output: output.close()