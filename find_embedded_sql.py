from argparse import ArgumentParser
from multiprocessing import Pool, freeze_support
from pathlib import Path
import uuid
import re
import shutil
import logging
import tempfile
import time
import traceback

from tqdm import tqdm
import pandas as pd


NUM_FILES_IN_BATCH = 100

DO_DEBUG = False

# TODO Update glob string as needed
GLOB_STR = '**/*'

# TODO Add reasons to skip as needed
REASONS_TO_SKIP_FILE = [
	lambda file_path: file_path.is_dir(),
	lambda file_path: '.git' in file_path.parts,
]

RESULTS_DF = Path('results.feather')

LOG_LEVEL = logging.INFO



# This is required b/c multiprocessing.pool.Pool.apply_async doesn't play nice
# when passed a method that hasn't been imported. One way around this is to create
# a class and it pass it one of *those* methods.
class FileProcessor:

	@staticmethod
	def process_files(tmp_dir,files,*args,**kwargs):
		
		logging.debug(f'Processing {len(files)} files')

		result_container = pd.DataFrame({
			'Filepath':[],
			'Sql':[]
		})

		try:

			for process_result, processing_exception in FileProcessor.yield_processing_results(files):

				if process_result is not None and not process_result.empty:

					result_container = FileProcessor.add_result_to_container(process_result,result_container)

				else:

					FileProcessor.process_exception(processing_exception)

			if not result_container.empty:

				file_path = FileProcessor.generate_temp_file_path(tmp_dir)

				FileProcessor.write_results(result_container,file_path)

				return file_path, None
			
			else:
				
				return None, None

		except Exception as e:

			return None, traceback.format_exc()

	@staticmethod
	def yield_processing_results(files,*args,**kwargs):
		
		logging.debug(f'Yielding processing results for {len(files)} files')

		for file in files:

			try:

				yield FileProcessor.process_file(file), None

			except Exception as e:

				yield None, traceback.format_exc()

	@staticmethod
	def add_result_to_container(process_result,result_container,*args,**kwargs):
    
		logging.debug(f'Adding result with {len(process_result)} entries to container with {len(result_container)} entries')

		return pd.concat([process_result,result_container],ignore_index=True)

	@staticmethod
	def process_exception(process_exec,*args,**kwargs):
    
		if process_exec:
      
			logging.debug('Processing Exception')
			
			logging.error(f'Exception while processing file:\n{process_exec}')

	@staticmethod
	def process_file(file,*args,**kwargs):
		
		logging.debug(f'Processing {file}')
		
		result_container = pd.DataFrame({
			'Filepath':[],
			'Sql':[]
		})      

		file_content = str(open(file,'rb').read()).upper()
		
		for match in re.finditer(r'SELECT .+?FROM.+?;',file_content):
			
			result_container.loc[len(result_container)] = [file.__str__(),match.group(0)]
			
		return result_container
		
	@staticmethod
	def generate_temp_file_path(tmp_dir,*args,**kwargs):
		"""Generates a file name and path within the given temp dir.

		Args:
			tmp_dir (Path): The temporary directory path.

		Returns:
			Path: The generated file path.
		"""

		logging.debug('Generating temp file path')

		while True:

			file_name = str(uuid.uuid4())

			file_path = Path(tmp_dir).joinpath(file_name)

			if not file_path.exists(): return file_path

	@staticmethod
	def write_results(result_container,file_path,*args,**kwargs):
    
		logging.debug(f'Writing {len(result_container)} results to {file_path}')

		result_container.to_feather(file_path)



def provide_output(prog_args,*args,**kwargs):

	logging.debug('Providing output')
    
	if RESULTS_DF.exists():
		
		df = pd.read_feather(RESULTS_DF)

		df['Filename'] = df['Filepath'].apply(lambda val: Path(val).name)

		filename_freqs = df.groupby('Filename').size().to_frame('Count').sort_values('Count',ascending=False).reset_index()

		filename_freqs = filename_freqs.head(10) if len(filename_freqs) >= 10 else filename_freqs

		filename_freqs = filename_freqs.to_string()

		freq_data = df.groupby('Filepath').size().to_frame('Count').reset_index().describe().to_string()

		print(f'''
The results were saved to {RESULTS_DF.__str__()}

There were {len(df)} sql statements found in {len(df["Filepath"].unique())} files.

Here's the top 10 filenames with sql statements:

{filename_freqs}

Here's the stats re: Result counts:

{freq_data}
''')

	else:
		
		print('No sql statements found.')

def process_exception(future_exception,*args,**kwargs):
    
	if future_exception:

		logging.debug('Processing exception')
		
		print(f'Exception while processing future')
		
		print(future_exception)

def save_results(file_path,*args,**kwargs):
    
	logging.debug(f'Saving results to {file_path}.')

	logging.debug(f'[Exists? > {file_path.exists()}] {file_path}')

	logging.debug(f'[Exists? > {RESULTS_DF.exists()}] {RESULTS_DF}')
	
	if file_path.exists():

		if RESULTS_DF.exists():
			
			pd.concat([pd.read_feather(RESULTS_DF),pd.read_feather(file_path)],ignore_index=True).to_feather(RESULTS_DF)
			
		else:
			
			pd.read_feather(file_path).to_feather(RESULTS_DF)

	else:
	
		print(f'{file_path.__str__()} doesn\'t exist!')

def yield_completed_futures(futures,*args,**kwargs):
    
	logging.debug(f'Yielding {len(futures)} completed futures')

	pbar = tqdm(total=len(futures),leave=False)

	sleep_time = (NUM_FILES_IN_BATCH / 10) * 2

	while len(futures) > 0:

		futures_to_remove = []

		for future in futures:

			if future.ready():

				yield future

				futures_to_remove.append(future)

				pbar.update(1)

		for future in futures_to_remove:

			futures.remove(future)

		if len(futures) > 0:

			time.sleep(sleep_time)

	pbar.close()

def yield_files(prog_args,*args,**kwargs):
    
	logging.debug('Yielding files')

	for file in tqdm(prog_args.target_loc.glob(GLOB_STR),leave=False,desc='Files'):

		if any([reason_to_skip_file(file) for reason_to_skip_file in REASONS_TO_SKIP_FILE]) or (prog_args.target_exts is not None and file.suffix not in prog_args.target_exts):

			continue

		yield file

def yield_file_batches(prog_args,*args,**kwargs):
    
	logging.debug(f'Yielding file batches of size {NUM_FILES_IN_BATCH}')

	files = []

	for file in tqdm(yield_files(prog_args),leave=False,desc='File Batches'):

		files.append(file)

		if len(files) >= NUM_FILES_IN_BATCH:

			yield files

			files = []

	if files: yield files

def gather_args(debug=False):

	logging.debug('Gathering args')

	arg_parser = ArgumentParser()

	arg_parser.add_argument('target_loc',type=Path)
	
	arg_parser.add_argument('--target_exts',nargs='*')

	# TODO ADD OTHER ARGS AS NECESSARY

	if debug:

		return arg_parser.parse_args(['TEST VAL TODO: REPLACE WITH USABLE VALUE'])

	else:

		return arg_parser.parse_args()

if __name__ == "__main__":

	# This is required for using PyInstaller to convert a script
	# using multiprocessing into an .exe
	freeze_support()
 
	logging.basicConfig(level=LOG_LEVEL)
 
	if RESULTS_DF.exists(): RESULTS_DF.unlink()
 
	with tempfile.TemporaryDirectory() as tmp_dir:
    
		print(tmp_dir)

		try:
		
			prog_args = gather_args(debug=DO_DEBUG)

			with Pool() as pool:

				futures = []

				for files in yield_file_batches(prog_args):

					futures.append(pool.apply_async(FileProcessor.process_files,(tmp_dir,files)))

				for future in yield_completed_futures(futures):

					file_path, future_exception = future.get()

					if file_path:

						save_results(file_path)

						file_path.unlink()

					else:

						process_exception(future_exception)

			provide_output(prog_args)

		except Exception as e:

			traceback.print_exc()







