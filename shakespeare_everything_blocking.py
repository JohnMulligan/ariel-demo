import sqlite3
from optparse import OptionParser, Option, OptionValueError
import re
import os
import gc
import psutil
import glob
import sys
import time
import json
import numpy
from mpi4py import MPI
import signal

parser = OptionParser()
parser.add_option("-b", action="store", type="int", dest="b", default=3)
(options, args) = parser.parse_args()


batch_size = options.b
comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size
status = MPI.Status()
lines_dict = json.loads(open("lines_dict.json","r").read())

interrupted = False
#code from http://www.cism.ucl.ac.be/Services/Formations/checkpointing.pdf
def signal_handler(signum, frame):
	global interrupted
	interrupted = True
	
signal.signal(signal.SIGINT, signal_handler)


def checkmem(id,verbose=0):
	proc = psutil.Process(os.getpid())
	mem = proc.memory_info()
	gc.collect()
	###mem = [mem.rss]
	###percent = psutil.virtual_memory().percent
	if verbose==1:
		print "proc: %s time: %s mem: %s" %(str(id),str(time.time()), mem)
	return mem.rss


def main():
	
	
	if rank == 0:
		data = numpy.arange(batch_size)
		jstor_shakespeare_db = sqlite3.connect('shakespeare.db')
		jstor_shakespeare_cursor = jstor_shakespeare_db.cursor()
		t=time.time()
		while True:
			print rank, time.time()-t, time.time()
			t=time.time()
			comm.Recv(data,source=MPI.ANY_SOURCE,status=status)
			source_worker = status.Get_source()
			next_batch = numpy.array([i for i in jstor_shakespeare_cursor.execute("SELECT rowid FROM matches WHERE mpi_mapped=0 ORDER BY RANDOM();").fetchmany(batch_size)])
			jstor_shakespeare_cursor.executemany("UPDATE matches SET mpi_mapped = 1 WHERE rowid = ?",next_batch)
			jstor_shakespeare_db.commit()
			comm.Ssend(next_batch,dest=source_worker)
			checkmem(rank,1)
			if interrupted:
				print "checkpointer exiting early"
				break
	
	
	
	elif rank in [1,2]:
		dbnames = ['ariel_home.db','ariel_foreign.db']
		this_write_cnx = sqlite3.connect(dbnames[rank-1],check_same_thread=False)
		this_write_cursor = this_write_cnx.cursor()
		t=time.time()
		while True:
			##Let this one run up until the last second -- no interruption stops.
			print rank, time.time()-t,time.time()
			t=time.time()
			
			comm.Probe(MPI.ANY_SOURCE,MPI.ANY_TAG,status)
			count = status.Get_elements(MPI.DOUBLE)
			data = numpy.empty([count/3,3],dtype=int)
			source_worker = status.Get_source()
			comm.Recv(data,source_worker)
			this_write_cursor.executemany("INSERT OR IGNORE INTO lines_and_docs_matches(source_line_id, target_line_id,doc_id) VALUES(?,?,?)", data.tolist())
			this_write_cnx.commit()
			checkmem(rank,1)
			del data
			
	
	else:
		
		jstor_match_rowids = numpy.arange(batch_size)
		comm.Ssend(numpy.arange(batch_size),dest=0)
		comm.Recv(jstor_match_rowids,source=0)
		jstor_shakespeare_db = sqlite3.connect('shakespeare_clean.db',check_same_thread=False)
		ariel_check = sqlite3.connect('ariel.db',check_same_thread=False)
		jstor_shakespeare_cursor = jstor_shakespeare_db.cursor()
		ariel_cursor = ariel_check.cursor()
		t=time.time()
		while True:
			print rank, time.time()-t,time.time()
			t=time.time()
			
			home_line_to_line_matches=[]
			foreign_line_to_line_matches=[]
			for jstor_match_rowid in jstor_match_rowids:
				#get the doc and the line on the match, which will serve as our "source" line
				try:
					node_data = jstor_shakespeare_cursor.execute("SELECT docid,line FROM matches WHERE rowid = ?;", [jstor_match_rowid]).fetchone()
					node_doc=node_data[0]
					source_line = node_data[1]
					
					#correct for the jstor/folger discrepancies
					#if this is one of jstor's bad line designators, look up the correct name in my custom dictionary/map
					try:
						source_line = str(lines_dict[source_line][1])
					except:
						pass
				
					#get the list of other lines cited in that document, which will serve as our "target" lines
					try:
						target_lines_jstor = list(set([str(i[0]) for i in jstor_shakespeare_cursor.execute("SELECT line FROM matches WHERE docid = ?;", [node_doc]).fetchall()]))
						target_lines_jstor = [i if i not in lines_dict.keys() else str(lines_dict[i][1]) for i in target_lines_jstor]
						target_lines_jstor.remove(source_line)
					except:
						print "error looking up target lines on doc %s" %node_doc
				
					try:
						#find their rowids in ariel
						source_line_rowid = ariel_cursor.execute("SELECT line_id FROM lines WHERE line = '%s'" %source_line).fetchone()
						doc_rowid = ariel_cursor.execute("SELECT doc_id FROM docs WHERE doi = '%s'" %node_doc).fetchone()[0]
						source_line_rowid = source_line_rowid[0]
						query ="SELECT line_id FROM lines WHERE line in (%s);" %(str(target_lines_jstor)[1:-1])
						target_line_rowid_results = [[min([i[0],source_line_rowid]),max([i[0],source_line_rowid]),doc_rowid] for i in ariel_cursor.execute(query).fetchall()]
						#determine which go to home and which go to foreign, while sorting them into these matrices
						home_line_to_line_matches += [i for i in target_line_rowid_results if i[0] == source_line_rowid]
						foreign_line_to_line_matches += [i for i in target_line_rowid_results if i[0] != source_line_rowid]
					except:
						print "error looking up source line %s" %source_line
				except:
					print "error looking up data on match %s" %str(jstor_match_rowid)
					print jstor_match_rowids,rank
				
			
			#send those matrices to the appropriate destination
			#but do it in a random order
			rand = numpy.random.randint(2,size=1)[0]
			
			for x in [0,-1]:
				this_array = [[home_line_to_line_matches,1],[foreign_line_to_line_matches,2]][x+rand]
				if len(this_array[0])>0:
					comm.Ssend(numpy.array(this_array[0]),dest=this_array[1])
			
			#get new batch of work if the job hasn't timed out

			comm.Ssend(numpy.arange(batch_size),dest=0)
			comm.Recv(jstor_match_rowids,source=0)
			checkmem(rank,1)
		
		




if __name__ == '__main__':
	main()
