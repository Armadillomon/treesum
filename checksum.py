import binascii
import time
import datetime
import pathlib
import os
import sys


class bytesize(int):
	PREFIXES = ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi")
	def __str__(self):
		i = min(len(self.__class__.PREFIXES) - 1, (self.bit_length() - 1) // 10)
		p = self.__class__.PREFIXES[i]
		return f"{self / 1024**i:.1f}{p}B"

def checksum_block(file, size=4096):
	crc = 0
	while data := file.read(size): crc = binascii.crc32(data, crc)
	return format(crc, "08X")

class DirTree:
	def __init__(self, root):
		self.root = pathlib.Path(root).resolve()
		self._errorlog = sys.stderr

	def walk(self, restart=None):
		self._skipped = 0
		self._processed = 0
		self._erroneous = 0
		self._processed_bytes = 0
		self._tree = os.walk(self.root)
		if restart:
			restart = (self.root / restart).resolve(True)
			self._restartfile = restart
			for path,dirs,files in self._tree:
				path = pathlib.Path(path)
				if path == self._restartfile: restart = None
				for file in files:
					fullpath = (path / file).resolve()
					if fullpath == self._restartfile: restart = None
					if restart: self._skipped += 1
					else:
						try:
							yield self._procfile(fullpath)
						except Exception as exc:
							print(f"Error processing file {exc.args[0]}: {exc.__cause__}", file=self._errorlog)
							continue
				if not restart: break
		for path,dirs,files in self._tree:
			path = pathlib.Path(path)
			for file in files:
				fullpath = (path / file).resolve()
				try:
					yield self._procfile(fullpath)
				except Exception as exc:
					print(f"Error processing file {exc.args[0]}: {exc.__cause__}", file=self._errorlog)
					continue

	def run(self, out, restart=None, errorlog=sys.stderr):
		self._errorlog = errorlog
		print(f"ROOT: {self.root}", file=out)
		t1 = time.perf_counter()
		for entry in self.walk(restart):
			print("{},{},{}".format(*entry), file=out)
		t2 = time.perf_counter()
		total_time = t2 - t1
		speed = self._processed_bytes / total_time
		print(f"Skipped files: {self._skipped}\nProcessed files: {self._processed}\nErrors: {self._erroneous}\nElapsed time: {datetime.timedelta(seconds=total_time)}\nBytes processed: {self._processed_bytes}B\nSpeed: {bytesize(speed)}/s")
		
	def _procfile(self, path):
		try:
			f = open(path, "rb", 0)
			size = os.path.getsize(path)
			checksum = checksum_block(f)
		except Exception as exc:
			self._erroneous += 1 
			raise RuntimeError(path) from exc	
		else:
			f.close()			
			self._processed += 1
			self._processed_bytes += size
			return (path, size, checksum)

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument("root", type=str, help="target directory")
	parser.add_argument("-o", "--output", type=str, help="output file")
	parser.add_argument("-e", "--error", type=str, help="error log file")
	parser.add_argument("-r", "--resume", type=str, help="resume traversing the tree from this file or directory")
	args = parser.parse_args()
	root = args.root
	if args.output: output = open(args.output, "w", encoding="UTF-8")
	else: output = sys.stdout
	if args.error: error = open(args.error, "w", encoding="UTF-8")
	else: error = sys.stderr
	tree = DirTree(root)
	tree.run(output, args.resume, error)
	if args.error: error.close()
	if args.output: output.close()	