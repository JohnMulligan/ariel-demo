import re
from optparse import OptionParser, Option, OptionValueError
import os
import plotly
from plotly.graph_objs import Scatter, Layout
import time


parser = OptionParser()
parser.add_option("-s", action="store", type="str", dest="s")
(options, args) = parser.parse_args()





def slurm_output_to_csv(slurm_id):

	d = open("slurm-%s.out" %slurm_id,'r')
	lines = d.readlines()
	
	proc_pattern = re.compile("^proc: [0-9]+")
	looptime_pattern = re.compile("^[0-9]+ [0-9]+")
	
	write = True
	
	os.mkdir(slurm_id)
	
	for l in lines:
		
		try:
			if proc_pattern.match(l):
				#parse memory line
			
				process = re.search("(?<=^proc: )[0-9]+",l).group(0)
				timestamp = re.search("(?<=time: )[0-9]+\.[0-9]+",l).group(0)
				mem = re.search("pmem\(.*\)",l).group(0)
				value = re.search("(?<=rss=)[0-9]+",mem).group(0)
				tag="mem"
				write=True
			elif looptime_pattern.match(l):
				#parse loop time line
				process,value,timestamp = l.split(' ')
				tag="looptime"
				write=True
			else:
				print "did not process data from %s" %l
				write=False
		except:
			print "did not process data from %s" %l
			write=False
			
		if write==True:
		
			e = open('./%s/proc-%s-%s.csv' %(slurm_id,process,tag),'a')
			output = '%s,%s,%s' %(process,value,timestamp)
			output = output.strip() + "\n"
		
			e.write(output)
			e.close()









def csv_to_plotly(dir):
	#step 1 get csv list
	
	modelookup = {"speed":"looptime.csv","mem":"mem.csv"}
	
	
	for mode in ['speed','mem']:
		
	
	
		flist = [i for i in os.listdir('%s' %dir) if modelookup[mode] in i]
		#iterate over csvs, use the first to find x value ranges
	
		##The below was for when I thought I was going to have to slice the data into different time chunks. Not necessary, it turns out.
		'''d = open('%s/%s' %(dir,flist[0]),'r')
		lines = d.readlines()
		number_entries = len(lines)
		match_slices = [[(i-1)*(number_entries/(nslices))+1,(i*(number_entries/nslices))] for i in range(1,nslices+1)]
		match_slices[-1] = [match_slices[-1][0],number_entries-1]
		ranges = [[float(lines[i[0]].split(',')[2].strip()),float(lines[i[1]].split(',')[2].strip())] for i in match_slices]
		ranges[0] = [ranges[0][0]-1000,ranges[0][1]]
		ranges[-1]= [ranges[-1][0],ranges[-1][1]+1000]
		d.close()'''
	
	
		d = open('%s/%s' %(dir,flist[0]),'r')
		lines = d.readlines()
		start_time = float(lines[0].split(',')[2])
		end_time = float(lines[-1].split(',')[2])
		d.close()
	
		databuffer = []

	
		st = time.gmtime(start_time)
		month=['January','February','March','April','May','June','July','August','September','October','November','December'][st.tm_mon -1]
		monthday=str(st.tm_mday)
		year = str(st.tm_year)
		start_seconds = str(st.tm_sec)
		print st.tm_sec
		if st.tm_sec <10:
			start_seconds = "0%s" %start_seconds
		start_hours = st.tm_hour
		start_minutes = st.tm_min
	
		start_time_formatted = "%d:%d:%d" %(st.tm_hour,st.tm_min,st.tm_sec)
	
	

		for f in flist:
			d = open('%s/%s' %(dir,f),'r')
			lines = d.readlines()
		
			process = re.search("[0-9]+",f).group(0)
		
			x_vals = []
			y_vals = []
	
			for l in lines:
				values = [float(i.strip()) for i in l.split(',')]
				recorded_time = values[2]
			
				if recorded_time > end_time:
					end_time = recorded_time
			
				x = recorded_time-start_time
				y = values[1]
			
				x_vals.append(x)
				y_vals.append(y)
	
			databuffer.append(Scatter(x=x_vals,y=y_vals,name="Process %s" %process))
	
			d.close()
	
		elapsed_hours = str(round((end_time-start_time)/3600,2))
		plotly.offline.plot({
		"data":databuffer,
		"layout": Layout(title="MPI %s monitoring test %s %s %s start time=%s:%s:%s (gmt) logged running time=%s (hours)" %(mode,month,monthday,year,start_hours,start_minutes,start_seconds,elapsed_hours))},
		filename="%s-%s.html" %(slurm_id,mode))

		
	
if __name__ == '__main__':
	slurm_id = str(options.s)
	slurm_output_to_csv(slurm_id)
	csv_to_plotly(slurm_id)